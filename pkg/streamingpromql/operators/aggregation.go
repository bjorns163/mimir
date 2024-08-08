// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Aggregation struct {
	Inner                    types.InstantVectorOperator
	Start                    int64 // Milliseconds since Unix epoch
	End                      int64 // Milliseconds since Unix epoch
	Interval                 int64 // In milliseconds
	Steps                    int
	Grouping                 []string // If this is a 'without' aggregation, NewAggregation will ensure that this slice contains __name__.
	Without                  bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	Annotations *annotations.Annotations

	metricNames        *MetricNames
	currentSeriesIndex int

	expressionPosition posrange.PositionRange
	emitAnnotationFunc functions.EmitAnnotationFunc

	remainingInnerSeriesToGroup []*group // One entry per series produced by Inner, value is the group for that series
	remainingGroups             []*group // One entry per group, in the order we want to return them

	haveEmittedMixedFloatsAndHistogramsWarning bool
}

func NewAggregation(
	inner types.InstantVectorOperator,
	start time.Time,
	end time.Time,
	interval time.Duration,
	grouping []string,
	without bool,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) *Aggregation {
	s, e, i := timestamp.FromTime(start), timestamp.FromTime(end), interval.Milliseconds()

	if without {
		labelsToDrop := make([]string, 0, len(grouping)+1)
		labelsToDrop = append(labelsToDrop, labels.MetricName)
		labelsToDrop = append(labelsToDrop, grouping...)
		grouping = labelsToDrop
	}

	slices.Sort(grouping)

	a := &Aggregation{
		Inner:                    inner,
		Start:                    s,
		End:                      e,
		Interval:                 i,
		Steps:                    stepCount(s, e, i),
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Annotations:              annotations,
		metricNames:              &MetricNames{},
		expressionPosition:       expressionPosition,
	}

	a.emitAnnotationFunc = a.emitAnnotation // This is an optimisation to avoid creating the EmitAnnotationFunc instance on every usage.

	return a
}

type groupWithLabels struct {
	labels labels.Labels
	group  *group
}

type group struct {
	// The number of input series that belong to this group that we haven't yet seen.
	remainingSeriesCount uint

	// The index of the last series that contributes to this group.
	// Used to sort groups in the order that they'll be completed in.
	lastSeriesIndex int

	// Sum, presence, and histograms for each step.
	floatSums              []float64
	floatCompensatingMeans []float64 // Mean, or "compensating value" for Kahan summation.
	floatPresent           []bool
	histogramSums          []*histogram.FloatHistogram
	histogramPointCount    int
}

var _ types.InstantVectorOperator = &Aggregation{}

var groupPool = zeropool.New(func() *group {
	return &group{}
})

func (a *Aggregation) ExpressionPosition() posrange.PositionRange {
	return a.expressionPosition
}

func (a *Aggregation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Fetch the source series
	innerSeries, err := a.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	defer types.PutSeriesMetadataSlice(innerSeries)

	if len(innerSeries) == 0 {
		// No input series == no output series.
		return nil, nil
	}

	a.metricNames.CaptureMetricNames(innerSeries)

	// Determine the groups we'll return.
	// Note that we use a string here to uniquely identify the groups, while Prometheus' engine uses a hash without any handling of hash collisions.
	// While rare, this may cause differences in the results returned by this engine and Prometheus' engine.
	groups := map[string]groupWithLabels{}
	groupLabelsBytesFunc, groupLabelsFunc := a.seriesToGroupFuncs()
	a.remainingInnerSeriesToGroup = make([]*group, 0, len(innerSeries))

	for seriesIdx, series := range innerSeries {
		groupLabelsString := groupLabelsBytesFunc(series.Labels)
		g, groupExists := groups[string(groupLabelsString)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			g.labels = groupLabelsFunc(series.Labels)
			g.group = groupPool.Get()
			g.group.remainingSeriesCount = 0

			groups[string(groupLabelsString)] = g
		}

		g.group.remainingSeriesCount++
		g.group.lastSeriesIndex = seriesIdx
		a.remainingInnerSeriesToGroup = append(a.remainingInnerSeriesToGroup, g.group)
	}

	// Sort the list of series we'll return, and maintain the order of the corresponding groups at the same time
	seriesMetadata := types.GetSeriesMetadataSlice(len(groups))
	a.remainingGroups = make([]*group, 0, len(groups))

	for _, g := range groups {
		seriesMetadata = append(seriesMetadata, types.SeriesMetadata{Labels: g.labels})
		a.remainingGroups = append(a.remainingGroups, g.group)
	}

	sort.Sort(groupSorter{seriesMetadata, a.remainingGroups})

	return seriesMetadata, nil
}

// seriesToGroupLabelsBytesFunc is a function that computes a string-like representation of the output group labels for the given input series.
//
// It returns a byte slice rather than a string to make it possible to avoid unnecessarily allocating a string.
//
// The byte slice returned may contain non-printable characters.
//
// Why not just use the labels.Labels computed by the seriesToGroupLabelsFunc and call String() on it?
//
// Most of the time, we don't need the labels.Labels instance, as we expect there are far fewer output groups than input series,
// and we only need the labels.Labels instance once per output group.
// However, we always need to compute the string-like representation for each input series, so we can look up its corresponding
// output group. And we can do this without allocating a string by returning just the bytes that make up the string.
// There's not much point in using the hash of the group labels as we always need the string (or the labels.Labels) to ensure
// there are no hash collisions - so we might as well just go straight to the string-like representation.
//
// Furthermore, labels.Labels.String() doesn't allow us to reuse the buffer used when producing the string or to return a byte slice,
// whereas this method does.
// This saves us allocating a new buffer and string for every single input series, which has a noticeable performance impact.
type seriesToGroupLabelsBytesFunc func(labels.Labels) []byte

// seriesToGroupLabelsFunc is a function that returns the output group labels for the given input series.
type seriesToGroupLabelsFunc func(labels.Labels) labels.Labels

func (a *Aggregation) seriesToGroupFuncs() (seriesToGroupLabelsBytesFunc, seriesToGroupLabelsFunc) {
	switch {
	case a.Without:
		return a.groupingWithoutLabelsSeriesToGroupFuncs()
	case len(a.Grouping) == 0:
		return groupToSingleSeriesLabelsBytesFunc, groupToSingleSeriesLabelsFunc
	default:
		return a.groupingByLabelsSeriesToGroupFuncs()
	}
}

var groupToSingleSeriesLabelsBytesFunc = func(_ labels.Labels) []byte { return nil }
var groupToSingleSeriesLabelsFunc = func(_ labels.Labels) labels.Labels { return labels.EmptyLabels() }

// groupingWithoutLabelsSeriesToGroupFuncs returns grouping functions for aggregations that use 'without'.
func (a *Aggregation) groupingWithoutLabelsSeriesToGroupFuncs() (seriesToGroupLabelsBytesFunc, seriesToGroupLabelsFunc) {
	// Why 1024 bytes? It's what labels.Labels.String() uses as a buffer size, so we use that as a sensible starting point too.
	b := make([]byte, 0, 1024)
	bytesFunc := func(l labels.Labels) []byte {
		return l.BytesWithoutLabels(b, a.Grouping...) // NewAggregation will add __name__ to Grouping for 'without' aggregations, so no need to add it here.
	}

	lb := labels.NewBuilder(labels.EmptyLabels())
	labelsFunc := func(m labels.Labels) labels.Labels {
		lb.Reset(m)
		lb.Del(a.Grouping...) // NewAggregation will add __name__ to Grouping for 'without' aggregations, so no need to add it here.
		l := lb.Labels()
		return l
	}

	return bytesFunc, labelsFunc
}

// groupingByLabelsSeriesToGroupFuncs returns grouping functions for aggregations that use 'by'.
func (a *Aggregation) groupingByLabelsSeriesToGroupFuncs() (seriesToGroupLabelsBytesFunc, seriesToGroupLabelsFunc) {
	// Why 1024 bytes? It's what labels.Labels.String() uses as a buffer size, so we use that as a sensible starting point too.
	b := make([]byte, 0, 1024)
	bytesFunc := func(l labels.Labels) []byte {
		return l.BytesWithLabels(b, a.Grouping...)
	}

	lb := labels.NewBuilder(labels.EmptyLabels())
	labelsFunc := func(m labels.Labels) labels.Labels {
		lb.Reset(m)
		lb.Keep(a.Grouping...)
		l := lb.Labels()
		return l
	}

	return bytesFunc, labelsFunc
}

func (a *Aggregation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(a.remainingGroups) == 0 {
		// No more groups left.
		return types.InstantVectorSeriesData{}, types.EOS
	}

	// Determine next group to return
	thisGroup := a.remainingGroups[0]
	a.remainingGroups = a.remainingGroups[1:]

	// Iterate through inner series until the desired group is complete
	if err := a.accumulateUntilGroupComplete(ctx, thisGroup); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// Construct the group and return it
	seriesData, err := a.constructSeriesData(thisGroup, a.Start, a.Interval)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	groupPool.Put(thisGroup)
	return seriesData, nil
}

func (a *Aggregation) accumulateUntilGroupComplete(ctx context.Context, g *group) error {
	for g.remainingSeriesCount > 0 {
		s, err := a.Inner.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("exhausted series before all groups were completed: %w", err)
			}

			return err
		}

		thisSeriesGroup := a.remainingInnerSeriesToGroup[0]
		a.remainingInnerSeriesToGroup = a.remainingInnerSeriesToGroup[1:]
		if err := a.accumulateSeriesIntoGroup(s, thisSeriesGroup, a.Steps, a.Start, a.Interval); err != nil {
			return err
		}

		a.currentSeriesIndex++
	}
	return nil
}

// Sentinel value used to indicate a sample has seen an invalid combination of histograms and should be ignored.
//
// Invalid combinations include exponential and custom buckets, and histograms with incompatible custom buckets.
var invalidCombinationOfHistograms = &histogram.FloatHistogram{}

func (a *Aggregation) constructSeriesData(thisGroup *group, start int64, interval int64) (types.InstantVectorSeriesData, error) {
	floatPointCount := a.reconcileAndCountFloatPoints(thisGroup)
	var floatPoints []promql.FPoint
	var err error
	if floatPointCount > 0 {
		floatPoints, err = types.FPointSlicePool.Get(floatPointCount, a.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		for i, havePoint := range thisGroup.floatPresent {
			if havePoint {
				t := start + int64(i)*interval
				f := thisGroup.floatSums[i] + thisGroup.floatCompensatingMeans[i]
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: f})
			}
		}
	}

	var histogramPoints []promql.HPoint
	if thisGroup.histogramPointCount > 0 {
		histogramPoints, err = types.HPointSlicePool.Get(thisGroup.histogramPointCount, a.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		for i, h := range thisGroup.histogramSums {
			if h != nil && h != invalidCombinationOfHistograms {
				t := start + int64(i)*interval
				histogramPoints = append(histogramPoints, promql.HPoint{T: t, H: thisGroup.histogramSums[i]})
			}
		}
	}

	types.Float64SlicePool.Put(thisGroup.floatSums, a.MemoryConsumptionTracker)
	types.Float64SlicePool.Put(thisGroup.floatCompensatingMeans, a.MemoryConsumptionTracker)
	types.BoolSlicePool.Put(thisGroup.floatPresent, a.MemoryConsumptionTracker)
	types.HistogramSlicePool.Put(thisGroup.histogramSums, a.MemoryConsumptionTracker)
	thisGroup.floatSums = nil
	thisGroup.floatCompensatingMeans = nil
	thisGroup.floatPresent = nil
	thisGroup.histogramSums = nil
	thisGroup.histogramPointCount = 0

	return types.InstantVectorSeriesData{Floats: floatPoints, Histograms: histogramPoints}, nil
}

// reconcileAndCountFloatPoints will return the number of points with a float present.
// It also takes the opportunity whilst looping through the floats to check if there
// is a conflicting Histogram present. If both are present, an empty vector should
// be returned. So this method removes the float+histogram where they conflict.
func (a *Aggregation) reconcileAndCountFloatPoints(g *group) int {
	// It would be possible to calculate the number of points when constructing
	// the series groups. However, it requires checking each point at each input
	// series which is more costly than looping again here and just checking each
	// point of the already grouped series.
	// See: https://github.com/grafana/mimir/pull/8442
	// We also take two different approaches here: One with extra checks if we
	// have both Floats and Histograms present, and one without these checks
	// so we don't have to do it at every point.
	floatPointCount := 0
	if len(g.floatPresent) > 0 && len(g.histogramSums) > 0 {
		for idx, present := range g.floatPresent {
			if present {
				if g.histogramSums[idx] != nil {
					// If a mix of histogram samples and float samples, the corresponding vector element is removed from the output vector entirely
					// and a warning annotation is emitted.
					g.floatPresent[idx] = false
					g.histogramSums[idx] = nil
					g.histogramPointCount--

					if !a.haveEmittedMixedFloatsAndHistogramsWarning {
						a.Annotations.Add(annotations.NewMixedFloatsHistogramsAggWarning(a.Inner.ExpressionPosition()))

						// The warning description only varies based on the position of the expression this operator represents, so only emit it
						// once, to avoid unnecessary work if there are many instances of floats and histograms conflicting.
						a.haveEmittedMixedFloatsAndHistogramsWarning = true
					}
				} else {
					floatPointCount++
				}
			}
		}
	} else {
		for _, p := range g.floatPresent {
			if p {
				floatPointCount++
			}
		}
	}
	return floatPointCount
}

func (a *Aggregation) accumulateSeriesIntoGroup(s types.InstantVectorSeriesData, seriesGroup *group, steps int, start int64, interval int64) error {
	var err error
	if len(s.Floats) > 0 && seriesGroup.floatSums == nil {
		// First series with float values for this group, populate it.
		seriesGroup.floatSums, err = types.Float64SlicePool.Get(steps, a.MemoryConsumptionTracker)
		if err != nil {
			return err
		}

		seriesGroup.floatCompensatingMeans, err = types.Float64SlicePool.Get(steps, a.MemoryConsumptionTracker)
		if err != nil {
			return err
		}

		seriesGroup.floatPresent, err = types.BoolSlicePool.Get(steps, a.MemoryConsumptionTracker)
		if err != nil {
			return err
		}
		seriesGroup.floatSums = seriesGroup.floatSums[:steps]
		seriesGroup.floatCompensatingMeans = seriesGroup.floatCompensatingMeans[:steps]
		seriesGroup.floatPresent = seriesGroup.floatPresent[:steps]
	}

	if len(s.Histograms) > 0 && seriesGroup.histogramSums == nil {
		// First series with histogram values for this group, populate it.
		seriesGroup.histogramSums, err = types.HistogramSlicePool.Get(steps, a.MemoryConsumptionTracker)
		if err != nil {
			return err
		}
		seriesGroup.histogramSums = seriesGroup.histogramSums[:steps]
	}

	for _, p := range s.Floats {
		idx := (p.T - start) / interval
		seriesGroup.floatSums[idx], seriesGroup.floatCompensatingMeans[idx] = kahanSumInc(p.F, seriesGroup.floatSums[idx], seriesGroup.floatCompensatingMeans[idx])
		seriesGroup.floatPresent[idx] = true
	}

	var lastUncopiedHistogram *histogram.FloatHistogram

	for _, p := range s.Histograms {
		idx := (p.T - start) / interval

		if seriesGroup.histogramSums[idx] == invalidCombinationOfHistograms {
			// We've already seen an invalid combination of histograms at this timestamp. Ignore this point.
			continue
		} else if seriesGroup.histogramSums[idx] != nil {
			seriesGroup.histogramSums[idx], err = seriesGroup.histogramSums[idx].Add(p.H)
			if err != nil {
				// Unable to add histograms together (likely due to invalid combination of histograms). Make sure we don't emit a sample at this timestamp.
				seriesGroup.histogramSums[idx] = invalidCombinationOfHistograms
				seriesGroup.histogramPointCount--

				if err := functions.NativeHistogramErrorToAnnotation(err, a.emitAnnotationFunc); err != nil {
					// Unknown error: we couldn't convert the error to an annotation. Give up.
					return err
				}
			}
		} else if lastUncopiedHistogram == p.H {
			// We've already used this histogram for a previous point due to lookback.
			// Make a copy of it so we don't modify the other point.
			seriesGroup.histogramSums[idx] = p.H.Copy()
			seriesGroup.histogramPointCount++
		} else {
			// This is the first time we have seen this histogram.
			// It is safe to store it and modify it later without copying, as we'll make copies above if the same histogram is used for subsequent points.
			seriesGroup.histogramSums[idx] = p.H
			seriesGroup.histogramPointCount++
			lastUncopiedHistogram = p.H
		}
	}

	types.PutInstantVectorSeriesData(s, a.MemoryConsumptionTracker)
	seriesGroup.remainingSeriesCount--
	return nil
}

func (a *Aggregation) emitAnnotation(generator functions.AnnotationGenerator) {
	metricName := a.metricNames.GetMetricNameForSeries(a.currentSeriesIndex)
	a.Annotations.Add(generator(metricName, a.Inner.ExpressionPosition()))
}

func (a *Aggregation) Close() {
	a.Inner.Close()
}

type groupSorter struct {
	metadata []types.SeriesMetadata
	groups   []*group
}

func (g groupSorter) Len() int {
	return len(g.metadata)
}

func (g groupSorter) Less(i, j int) bool {
	return g.groups[i].lastSeriesIndex < g.groups[j].lastSeriesIndex
}

func (g groupSorter) Swap(i, j int) {
	g.metadata[i], g.metadata[j] = g.metadata[j], g.metadata[i]
	g.groups[i], g.groups[j] = g.groups[j], g.groups[i]
}

// TODO(jhesketh): This will likely be useful elsewhere, so we may move this in the future.
// (We could also consider exporting this from the upstream promql package).
func kahanSumInc(inc, sum, c float64) (newSum, newC float64) {
	t := sum + inc
	switch {
	case math.IsInf(t, 0):
		c = 0

	// Using Neumaier improvement, swap if next term larger than sum.
	case math.Abs(sum) >= math.Abs(inc):
		c += (sum - t) + inc
	default:
		c += (inc - t) + sum
	}
	return t, c
}
