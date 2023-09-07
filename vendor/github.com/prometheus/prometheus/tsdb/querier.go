// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"fmt"
	"math"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

type blockBaseQuerier struct {
	blockID    ulid.ULID
	index      IndexReader
	chunks     ChunkReader
	tombstones tombstones.Reader

	closed bool

	mint, maxt int64
}

func newBlockBaseQuerier(b BlockReader, mint, maxt int64) (*blockBaseQuerier, error) {
	indexr, err := b.Index()
	if err != nil {
		return nil, errors.Wrap(err, "open index reader")
	}
	chunkr, err := b.Chunks()
	if err != nil {
		indexr.Close()
		return nil, errors.Wrap(err, "open chunk reader")
	}
	tombsr, err := b.Tombstones()
	if err != nil {
		indexr.Close()
		chunkr.Close()
		return nil, errors.Wrap(err, "open tombstone reader")
	}

	if tombsr == nil {
		tombsr = tombstones.NewMemTombstones()
	}
	return &blockBaseQuerier{
		blockID:    b.Meta().ULID,
		mint:       mint,
		maxt:       maxt,
		index:      indexr,
		chunks:     chunkr,
		tombstones: tombsr,
	}, nil
}

func (q *blockBaseQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	res, err := q.index.SortedLabelValues(name, matchers...)
	return res, nil, err
}

func (q *blockBaseQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	res, err := q.index.LabelNames(matchers...)
	return res, nil, err
}

func (q *blockBaseQuerier) Close() error {
	if q.closed {
		return errors.New("block querier already closed")
	}

	errs := tsdb_errors.NewMulti(
		q.index.Close(),
		q.chunks.Close(),
		q.tombstones.Close(),
	)
	q.closed = true
	return errs.Err()
}

type blockQuerier struct {
	*blockBaseQuerier
}

// NewBlockQuerier returns a querier against the block reader and requested min and max time range.
func NewBlockQuerier(b BlockReader, mint, maxt int64) (storage.Querier, error) {
	q, err := newBlockBaseQuerier(b, mint, maxt)
	if err != nil {
		return nil, err
	}
	return &blockQuerier{blockBaseQuerier: q}, nil
}

func (q *blockQuerier) Select(sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.SeriesSet {
	mint := q.mint
	maxt := q.maxt
	disableTrimming := false
	sharded := hints != nil && hints.ShardCount > 0
	p, pendingMatchers, err := q.index.PostingsForMatchers(sharded, ms...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if sharded {
		p = q.index.ShardedPostings(p, hints.ShardIndex, hints.ShardCount)
	}
	if sortSeries {
		p = q.index.SortedPostings(p)
	}

	if hints != nil {
		mint = hints.Start
		maxt = hints.End
		disableTrimming = hints.DisableTrimming
		if hints.Func == "series" {
			// When you're only looking up metadata (for example series API), you don't need to load any chunks.
			return newFilteredSeriesSet(pendingMatchers, newBlockSeriesSet(q.index, newNopChunkReader(), q.tombstones, p, mint, maxt, disableTrimming))
		}
	}

	return newFilteredSeriesSet(pendingMatchers, newBlockSeriesSet(q.index, q.chunks, q.tombstones, p, mint, maxt, disableTrimming))
}

// blockChunkQuerier provides chunk querying access to a single block database.
type blockChunkQuerier struct {
	*blockBaseQuerier
}

// NewBlockChunkQuerier returns a chunk querier against the block reader and requested min and max time range.
func NewBlockChunkQuerier(b BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := newBlockBaseQuerier(b, mint, maxt)
	if err != nil {
		return nil, err
	}
	return &blockChunkQuerier{blockBaseQuerier: q}, nil
}

func (q *blockChunkQuerier) Select(sortSeries bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.ChunkSeriesSet {
	mint := q.mint
	maxt := q.maxt
	disableTrimming := false
	if hints != nil {
		mint = hints.Start
		maxt = hints.End
		disableTrimming = hints.DisableTrimming
	}
	sharded := hints != nil && hints.ShardCount > 0
	p, pendingMatchers, err := q.index.PostingsForMatchers(sharded, ms...) // TODO dimitarvdimitrov handle pendingMatchers
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}
	if sharded {
		p = q.index.ShardedPostings(p, hints.ShardIndex, hints.ShardCount)
	}
	if sortSeries {
		p = q.index.SortedPostings(p)
	}
	return newFilteredChunkSeriesSet(pendingMatchers, NewBlockChunkSeriesSet(q.blockID, q.index, q.chunks, q.tombstones, p, mint, maxt, disableTrimming))
}

type postingGroup struct {
	matcher       *labels.Matcher
	lVals         []string // TODO maybe also keep the posting offset so we don't have to lookup the postings offset table again
	isSubtracting bool
	numSeries     int
}

// PostingsForMatchers assembles a single postings iterator against the index reader
// based on the given matchers. The resulting postings are not ordered by series.
func PostingsForMatchers(ix IndexPostingsReader, ms ...*labels.Matcher) (_ index.Postings, pendingMatchers []*labels.Matcher, _ error) {
	// See which label must be non-empty.
	// Optimization for case like {l=~".", l!="1"}.
	labelMustBeSet := make(map[string]bool, len(ms))
	for _, m := range ms {
		if !m.Matches("") {
			labelMustBeSet[m.Name] = true
		}
	}
	postingGroups := make([]postingGroup, 0, len(ms))
	for _, m := range ms {
		switch {
		case m.Name == "" && m.Value == "": // Special-case for AllPostings, used in tests at least.
			_, v := index.AllPostingsKey()
			postingGroups = append(postingGroups, postingGroup{
				matcher:       m,
				lVals:         []string{v},
				isSubtracting: false,
			})
		case labelMustBeSet[m.Name]:
			// If this matcher must be non-empty, we can be smarter.
			matchesEmpty := m.Matches("")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp
			switch {
			case isNot && matchesEmpty: // l!="foo"
				inverse, err := m.Inverse()
				if err != nil {
					return nil, nil, err
				}
				vals, err := labelValuesForMatcher(ix, inverse)
				if err != nil {
					return nil, nil, err
				}
				postingGroups = append(postingGroups, postingGroup{
					matcher:       m,
					lVals:         vals,
					isSubtracting: true,
				})
			case isNot && !matchesEmpty: // l!=""
				vals, err := labelValuesForMatcher(ix, m)
				if err != nil {
					return nil, nil, err
				}
				postingGroups = append(postingGroups, postingGroup{
					matcher:       m,
					lVals:         vals,
					isSubtracting: false,
				})
			default: // l="a"
				// Regular matcher, use normal postingsForMatcher.
				vals, err := labelValuesForMatcher(ix, m)
				if err != nil {
					return nil, nil, err
				}
				if len(vals) == 0 {
					return index.EmptyPostings(), nil, nil
				}
				postingGroups = append(postingGroups, postingGroup{
					matcher:       m,
					lVals:         vals,
					isSubtracting: false,
				})
			}
		default: // l=""
			// If the matchers for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
			inverse, err := m.Inverse()
			if err != nil {
				return nil, nil, err
			}
			vals, err := labelValuesForMatcher(ix, inverse)
			if err != nil {
				return nil, nil, err
			}

			postingGroups = append(postingGroups, postingGroup{
				matcher:       m,
				lVals:         vals,
				isSubtracting: true,
			})
		}
	}

	// fetch the sizes for the posting groups
	for i, pg := range postingGroups {
		totalSize, err := ix.PostingsSizeEstimation(pg.matcher.Name, pg.lVals...)
		if err != nil {
			return nil, nil, err
		}
		postingGroups[i].numSeries = totalSize
	}

	slices.SortFunc(postingGroups, func(a, b postingGroup) bool {
		return a.numSeries < b.numSeries
	})

	// decide whether to exclude any posting groups which will have a lot of postings
	smallestIntersectingSize := math.MaxInt
	for _, pg := range postingGroups {
		if !pg.isSubtracting && pg.numSeries < smallestIntersectingSize {
			smallestIntersectingSize = pg.numSeries
		}
	}

	var (
		maxPostingListSize          = smallestIntersectingSize * 512 // about 512B per series
		selectedPostingGroupsSize   = 0
		atLeastOneIntersectingGroup = false
	)

	for i, pg := range postingGroups {
		if atLeastOneIntersectingGroup && selectedPostingGroupsSize+pg.numSeries > maxPostingListSize {
			pendingMatchers = extractMatchers(postingGroups[i:])
			postingGroups = postingGroups[:i]
			break
		}
		atLeastOneIntersectingGroup = atLeastOneIntersectingGroup || !pg.isSubtracting
		selectedPostingGroupsSize += pg.numSeries
	}

	var its, notIts []index.Postings
	for _, pg := range postingGroups {
		it, err := ix.Postings(pg.matcher.Name, pg.lVals...)
		if err != nil {
			return nil, nil, err
		}
		if pg.isSubtracting {
			notIts = append(notIts, it)
		} else {
			its = append(its, it)
		}
	}

	// If there's nothing to subtract from, add in everything and remove the notIts later.
	if len(its) == 0 && len(notIts) != 0 {
		k, v := index.AllPostingsKey()
		allPostings, err := ix.Postings(k, v)
		if err != nil {
			return nil, nil, err
		}
		its = append(its, allPostings)
	}

	it := index.Intersect(its...)

	for _, n := range notIts {
		it = index.Without(it, n)
	}

	return it, pendingMatchers, nil
}

func extractMatchers(groups []postingGroup) []*labels.Matcher {
	res := make([]*labels.Matcher, 0, len(groups))
	for _, g := range groups {
		res = append(res, g.matcher)
	}
	return res
}

func labelValuesForMatcher(ix IndexPostingsReader, m *labels.Matcher) ([]string, error) {
	// TODO dimitarvdimitrov cosnider keeping the shortcuts with SetMatchers and with MatchEqual
	vals, err := ix.LabelValues(m.Name)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, val := range vals {
		if m.Matches(val) {
			res = append(res, val)
		}
	}
	return res, nil
}

func postingsForMatcher(ix IndexPostingsReader, m *labels.Matcher) (index.Postings, error) {
	// This method will not return postings for missing labels.

	// Fast-path for equal matching.
	if m.Type == labels.MatchEqual {
		return ix.Postings(m.Name, m.Value)
	}

	// Fast-path for set matching.
	if m.Type == labels.MatchRegexp {
		setMatches := m.SetMatches()
		if len(setMatches) > 0 {
			return ix.Postings(m.Name, setMatches...)
		}
	}

	vals, err := ix.LabelValues(m.Name)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, val := range vals {
		if m.Matches(val) {
			res = append(res, val)
		}
	}

	if len(res) == 0 {
		return index.EmptyPostings(), nil
	}

	return ix.Postings(m.Name, res...)
}

// inversePostingsForMatcher returns the postings for the series with the label name set but not matching the matcher.
func inversePostingsForMatcher(ix IndexPostingsReader, m *labels.Matcher) (index.Postings, error) {
	// Fast-path for MatchNotRegexp matching.
	// Inverse of a MatchNotRegexp is MatchRegexp (double negation).
	// Fast-path for set matching.
	if m.Type == labels.MatchNotRegexp {
		setMatches := m.SetMatches()
		if len(setMatches) > 0 {
			return ix.Postings(m.Name, setMatches...)
		}
	}

	// Fast-path for MatchNotEqual matching.
	// Inverse of a MatchNotEqual is MatchEqual (double negation).
	if m.Type == labels.MatchNotEqual {
		return ix.Postings(m.Name, m.Value)
	}

	vals, err := ix.LabelValues(m.Name)
	if err != nil {
		return nil, err
	}

	var res []string
	// If the inverse match is ="", we just want all the values.
	if m.Type == labels.MatchEqual && m.Value == "" {
		res = vals
	} else {
		for _, val := range vals {
			if !m.Matches(val) {
				res = append(res, val)
			}
		}
	}

	return ix.Postings(m.Name, res...)
}

const maxExpandedPostingsFactor = 100 // Division factor for maximum number of matched series.

func labelValuesWithMatchers(r IndexReader, name string, matchers ...*labels.Matcher) ([]string, error) {
	p, _, err := PostingsForMatchers(r, matchers...)
	if err != nil {
		return nil, errors.Wrap(err, "fetching postings for matchers")
	}

	allValues, err := r.LabelValues(name)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching values of label %s", name)
	}

	// If we have a matcher for the label name, we can filter out values that don't match
	// before we fetch postings. This is especially useful for labels with many values.
	// e.g. __name__ with a selector like {__name__="xyz"}
	for _, m := range matchers {
		if m.Name != name {
			continue
		}

		// re-use the allValues slice to avoid allocations
		// this is safe because the iteration is always ahead of the append
		filteredValues := allValues[:0]
		for _, v := range allValues {
			if m.Matches(v) {
				filteredValues = append(filteredValues, v)
			}
		}
		allValues = filteredValues
	}

	// Let's see if expanded postings for matchers have smaller cardinality than label values.
	// Since computing label values from series is expensive, we apply a limit on number of expanded
	// postings (and series).
	maxExpandedPostings := len(allValues) / maxExpandedPostingsFactor
	if maxExpandedPostings > 0 {
		// Add space for one extra posting when checking if we expanded all postings.
		expanded := make([]storage.SeriesRef, 0, maxExpandedPostings+1)

		// Call p.Next() even if len(expanded) == maxExpandedPostings. This tells us if there are more postings or not.
		for len(expanded) <= maxExpandedPostings && p.Next() {
			expanded = append(expanded, p.At())
		}

		if len(expanded) <= maxExpandedPostings {
			// When we're here, p.Next() must have returned false, so we need to check for errors.
			if err := p.Err(); err != nil {
				return nil, errors.Wrap(err, "expanding postings for matchers")
			}

			// We have expanded all the postings -- all returned label values will be from these series only.
			// (We supply allValues as a buffer for storing results. It should be big enough already, since it holds all possible label values.)
			return labelValuesFromSeries(r, name, expanded, allValues)
		}

		// If we haven't reached end of postings, we prepend our expanded postings to "p", and continue.
		p = newPrependPostings(expanded, p)
	}

	valuesPostings := make([]index.Postings, len(allValues))
	for i, value := range allValues {
		valuesPostings[i], err = r.Postings(name, value)
		if err != nil {
			return nil, errors.Wrapf(err, "fetching postings for %s=%q", name, value)
		}
	}
	indexes, err := index.FindIntersectingPostings(p, valuesPostings)
	if err != nil {
		return nil, errors.Wrap(err, "intersecting postings")
	}

	values := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		values = append(values, allValues[idx])
	}

	return values, nil
}

// labelValuesFromSeries returns all unique label values from for given label name from supplied series. Values are not sorted.
// buf is space for holding result (if it isn't big enough, it will be ignored), may be nil.
func labelValuesFromSeries(r IndexReader, labelName string, refs []storage.SeriesRef, buf []string) ([]string, error) {
	values := map[string]struct{}{}

	var builder labels.ScratchBuilder
	for _, ref := range refs {
		err := r.Series(ref, &builder, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "label values for label %s", labelName)
		}

		v := builder.Labels().Get(labelName)
		if v != "" {
			values[v] = struct{}{}
		}
	}

	if cap(buf) >= len(values) {
		buf = buf[:0]
	} else {
		buf = make([]string, 0, len(values))
	}
	for v := range values {
		buf = append(buf, v)
	}
	return buf, nil
}

func newPrependPostings(a []storage.SeriesRef, b index.Postings) index.Postings {
	return &prependPostings{
		ix:     -1,
		prefix: a,
		rest:   b,
	}
}

// prependPostings returns series references from "prefix" before using "rest" postings.
type prependPostings struct {
	ix     int
	prefix []storage.SeriesRef
	rest   index.Postings
}

func (p *prependPostings) Next() bool {
	p.ix++
	if p.ix < len(p.prefix) {
		return true
	}
	return p.rest.Next()
}

func (p *prependPostings) Seek(v storage.SeriesRef) bool {
	for p.ix < len(p.prefix) {
		if p.ix >= 0 && p.prefix[p.ix] >= v {
			return true
		}
		p.ix++
	}

	return p.rest.Seek(v)
}

func (p *prependPostings) At() storage.SeriesRef {
	if p.ix >= 0 && p.ix < len(p.prefix) {
		return p.prefix[p.ix]
	}
	return p.rest.At()
}

func (p *prependPostings) Err() error {
	if p.ix >= 0 && p.ix < len(p.prefix) {
		return nil
	}
	return p.rest.Err()
}

func labelNamesWithMatchers(r IndexReader, matchers ...*labels.Matcher) ([]string, error) {
	p, _, err := r.PostingsForMatchers(false, matchers...) // TODO dimitarvdimitrov handle pendingMatchers
	if err != nil {
		return nil, err
	}

	var postings []storage.SeriesRef
	for p.Next() {
		postings = append(postings, p.At())
	}
	if p.Err() != nil {
		return nil, errors.Wrapf(p.Err(), "postings for label names with matchers")
	}

	return r.LabelNamesFor(postings...)
}

// seriesData, used inside other iterators, are updated when we move from one series to another.
type seriesData struct {
	chks      []chunks.Meta
	intervals tombstones.Intervals
	labels    labels.Labels
}

// Labels implements part of storage.Series and storage.ChunkSeries.
func (s *seriesData) Labels() labels.Labels { return s.labels }

// blockBaseSeriesSet allows to iterate over all series in the single block.
// Iterated series are trimmed with given min and max time as well as tombstones.
// See newBlockSeriesSet and NewBlockChunkSeriesSet to use it for either sample or chunk iterating.
type blockBaseSeriesSet struct {
	blockID         ulid.ULID
	p               index.Postings
	index           IndexReader
	chunks          ChunkReader
	tombstones      tombstones.Reader
	mint, maxt      int64
	disableTrimming bool

	curr seriesData

	bufChks []chunks.Meta
	builder labels.ScratchBuilder
	err     error
}

func (b *blockBaseSeriesSet) Next() bool {
	for b.p.Next() {
		if err := b.index.Series(b.p.At(), &b.builder, &b.bufChks); err != nil {
			// Postings may be stale. Skip if no underlying series exists.
			if errors.Cause(err) == storage.ErrNotFound {
				continue
			}
			b.err = errors.Wrapf(err, "get series %d", b.p.At())
			return false
		}

		if len(b.bufChks) == 0 {
			continue
		}

		intervals, err := b.tombstones.Get(b.p.At())
		if err != nil {
			b.err = errors.Wrap(err, "get tombstones")
			return false
		}

		// NOTE:
		// * block time range is half-open: [meta.MinTime, meta.MaxTime).
		// * chunks are both closed: [chk.MinTime, chk.MaxTime].
		// * requested time ranges are closed: [req.Start, req.End].

		var trimFront, trimBack bool

		// Copy chunks as iterables are reusable.
		// Count those in range to size allocation (roughly - ignoring tombstones).
		nChks := 0
		for _, chk := range b.bufChks {
			if !(chk.MaxTime < b.mint || chk.MinTime > b.maxt) {
				nChks++
			}
		}
		chks := make([]chunks.Meta, 0, nChks)

		// Prefilter chunks and pick those which are not entirely deleted or totally outside of the requested range.
		for _, chk := range b.bufChks {
			if chk.MaxTime < b.mint {
				continue
			}
			if chk.MinTime > b.maxt {
				continue
			}
			if (tombstones.Interval{Mint: chk.MinTime, Maxt: chk.MaxTime}.IsSubrange(intervals)) {
				continue
			}
			chks = append(chks, chk)

			// If still not entirely deleted, check if trim is needed based on requested time range.
			if !b.disableTrimming {
				if chk.MinTime < b.mint {
					trimFront = true
				}
				if chk.MaxTime > b.maxt {
					trimBack = true
				}
			}
		}

		if len(chks) == 0 {
			continue
		}

		if trimFront {
			intervals = intervals.Add(tombstones.Interval{Mint: math.MinInt64, Maxt: b.mint - 1})
		}
		if trimBack {
			intervals = intervals.Add(tombstones.Interval{Mint: b.maxt + 1, Maxt: math.MaxInt64})
		}

		b.curr.labels = b.builder.Labels()
		b.curr.chks = chks
		b.curr.intervals = intervals
		return true
	}
	return false
}

func (b *blockBaseSeriesSet) Err() error {
	if b.err != nil {
		return b.err
	}
	return b.p.Err()
}

func (b *blockBaseSeriesSet) Warnings() storage.Warnings { return nil }

// populateWithDelGenericSeriesIterator allows to iterate over given chunk
// metas. In each iteration it ensures that chunks are trimmed based on given
// tombstones interval if any.
//
// populateWithDelGenericSeriesIterator assumes that chunks that would be fully
// removed by intervals are filtered out in previous phase.
//
// On each iteration currChkMeta is available. If currDelIter is not nil, it
// means that the chunk iterator in currChkMeta is invalid and a chunk rewrite
// is needed, for which currDelIter should be used.
type populateWithDelGenericSeriesIterator struct {
	blockID ulid.ULID
	chunks  ChunkReader
	// chks are expected to be sorted by minTime and should be related to
	// the same, single series.
	chks []chunks.Meta

	i         int // Index into chks; -1 if not started yet.
	err       error
	bufIter   DeletedIterator // Retained for memory re-use. currDelIter may point here.
	intervals tombstones.Intervals

	currDelIter chunkenc.Iterator
	currChkMeta chunks.Meta
}

func (p *populateWithDelGenericSeriesIterator) reset(blockID ulid.ULID, cr ChunkReader, chks []chunks.Meta, intervals tombstones.Intervals) {
	p.blockID = blockID
	p.chunks = cr
	p.chks = chks
	p.i = -1
	p.err = nil
	p.bufIter.Iter = nil
	p.bufIter.Intervals = p.bufIter.Intervals[:0]
	p.intervals = intervals
	p.currDelIter = nil
	p.currChkMeta = chunks.Meta{}
}

// If copyHeadChunk is true, then the head chunk (i.e. the in-memory chunk of the TSDB)
// is deep copied to avoid races between reads and copying chunk bytes.
// However, if the deletion intervals overlaps with the head chunk, then the head chunk is
// not copied irrespective of copyHeadChunk because it will be re-encoded later anyway.
func (p *populateWithDelGenericSeriesIterator) next(copyHeadChunk bool) bool {
	if p.err != nil || p.i >= len(p.chks)-1 {
		return false
	}

	p.i++
	p.currChkMeta = p.chks[p.i]

	p.bufIter.Intervals = p.bufIter.Intervals[:0]
	for _, interval := range p.intervals {
		if p.currChkMeta.OverlapsClosedInterval(interval.Mint, interval.Maxt) {
			p.bufIter.Intervals = p.bufIter.Intervals.Add(interval)
		}
	}

	hcr, ok := p.chunks.(*headChunkReader)
	if ok && copyHeadChunk && len(p.bufIter.Intervals) == 0 {
		// ChunkWithCopy will copy the head chunk.
		var maxt int64
		p.currChkMeta.Chunk, maxt, p.err = hcr.ChunkWithCopy(p.currChkMeta)
		// For the in-memory head chunk the index reader sets maxt as MaxInt64. We fix it here.
		p.currChkMeta.MaxTime = maxt
	} else {
		p.currChkMeta.Chunk, p.err = p.chunks.Chunk(p.currChkMeta)
	}
	if p.err != nil {
		p.err = errors.Wrapf(p.err, "cannot populate chunk %d from block %s", p.currChkMeta.Ref, p.blockID.String())
		return false
	}

	if len(p.bufIter.Intervals) == 0 {
		// If there is no overlap with deletion intervals, we can take chunk as it is.
		p.currDelIter = nil
		return true
	}

	// We don't want the full chunk, take just a part of it.
	p.bufIter.Iter = p.currChkMeta.Chunk.Iterator(p.bufIter.Iter)
	p.currDelIter = &p.bufIter
	return true
}

func (p *populateWithDelGenericSeriesIterator) Err() error { return p.err }

type blockSeriesEntry struct {
	chunks  ChunkReader
	blockID ulid.ULID
	seriesData
}

func (s *blockSeriesEntry) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	pi, ok := it.(*populateWithDelSeriesIterator)
	if !ok {
		pi = &populateWithDelSeriesIterator{}
	}
	pi.reset(s.blockID, s.chunks, s.chks, s.intervals)
	return pi
}

type chunkSeriesEntry struct {
	chunks  ChunkReader
	blockID ulid.ULID
	seriesData
}

func (s *chunkSeriesEntry) Iterator(it chunks.Iterator) chunks.Iterator {
	pi, ok := it.(*populateWithDelChunkSeriesIterator)
	if !ok {
		pi = &populateWithDelChunkSeriesIterator{}
	}
	pi.reset(s.blockID, s.chunks, s.chks, s.intervals)
	return pi
}

func (s *chunkSeriesEntry) ChunkCount() (int, error) {
	return len(s.chks), nil
}

// populateWithDelSeriesIterator allows to iterate over samples for the single series.
type populateWithDelSeriesIterator struct {
	populateWithDelGenericSeriesIterator

	curr chunkenc.Iterator
}

func (p *populateWithDelSeriesIterator) reset(blockID ulid.ULID, cr ChunkReader, chks []chunks.Meta, intervals tombstones.Intervals) {
	p.populateWithDelGenericSeriesIterator.reset(blockID, cr, chks, intervals)
	p.curr = nil
}

func (p *populateWithDelSeriesIterator) Next() chunkenc.ValueType {
	if p.curr != nil {
		if valueType := p.curr.Next(); valueType != chunkenc.ValNone {
			return valueType
		}
	}

	for p.next(false) {
		if p.currDelIter != nil {
			p.curr = p.currDelIter
		} else {
			p.curr = p.currChkMeta.Chunk.Iterator(p.curr)
		}
		if valueType := p.curr.Next(); valueType != chunkenc.ValNone {
			return valueType
		}
	}
	return chunkenc.ValNone
}

func (p *populateWithDelSeriesIterator) Seek(t int64) chunkenc.ValueType {
	if p.curr != nil {
		if valueType := p.curr.Seek(t); valueType != chunkenc.ValNone {
			return valueType
		}
	}
	for p.Next() != chunkenc.ValNone {
		if valueType := p.curr.Seek(t); valueType != chunkenc.ValNone {
			return valueType
		}
	}
	return chunkenc.ValNone
}

func (p *populateWithDelSeriesIterator) At() (int64, float64) {
	return p.curr.At()
}

func (p *populateWithDelSeriesIterator) AtHistogram() (int64, *histogram.Histogram) {
	return p.curr.AtHistogram()
}

func (p *populateWithDelSeriesIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return p.curr.AtFloatHistogram()
}

func (p *populateWithDelSeriesIterator) AtT() int64 {
	return p.curr.AtT()
}

func (p *populateWithDelSeriesIterator) Err() error {
	if err := p.populateWithDelGenericSeriesIterator.Err(); err != nil {
		return err
	}
	if p.curr != nil {
		return p.curr.Err()
	}
	return nil
}

type populateWithDelChunkSeriesIterator struct {
	populateWithDelGenericSeriesIterator

	curr chunks.Meta
}

func (p *populateWithDelChunkSeriesIterator) reset(blockID ulid.ULID, cr ChunkReader, chks []chunks.Meta, intervals tombstones.Intervals) {
	p.populateWithDelGenericSeriesIterator.reset(blockID, cr, chks, intervals)
	p.curr = chunks.Meta{}
}

func (p *populateWithDelChunkSeriesIterator) Next() bool {
	if !p.next(true) {
		return false
	}
	p.curr = p.currChkMeta
	if p.currDelIter == nil {
		return true
	}
	valueType := p.currDelIter.Next()
	if valueType == chunkenc.ValNone {
		if err := p.currDelIter.Err(); err != nil {
			p.err = errors.Wrap(err, "iterate chunk while re-encoding")
		}
		return false
	}

	// Re-encode the chunk if iterator is provider. This means that it has
	// some samples to be deleted or chunk is opened.
	var (
		newChunk chunkenc.Chunk
		app      chunkenc.Appender
		t        int64
		err      error
	)
	switch valueType {
	case chunkenc.ValHistogram:
		newChunk = chunkenc.NewHistogramChunk()
		if app, err = newChunk.Appender(); err != nil {
			break
		}

		for vt := valueType; vt != chunkenc.ValNone; vt = p.currDelIter.Next() {
			if vt != chunkenc.ValHistogram {
				err = fmt.Errorf("found value type %v in histogram chunk", vt)
				break
			}
			var h *histogram.Histogram
			t, h = p.currDelIter.AtHistogram()
			_, _, app, err = app.AppendHistogram(nil, t, h, true)
			if err != nil {
				break
			}
		}
	case chunkenc.ValFloat:
		newChunk = chunkenc.NewXORChunk()
		if app, err = newChunk.Appender(); err != nil {
			break
		}
		var v float64
		t, v = p.currDelIter.At()
		p.curr.MinTime = t
		app.Append(t, v)
		for vt := p.currDelIter.Next(); vt != chunkenc.ValNone; vt = p.currDelIter.Next() {
			if vt != chunkenc.ValFloat {
				err = fmt.Errorf("found value type %v in float chunk", vt)
				break
			}
			t, v = p.currDelIter.At()
			app.Append(t, v)
		}
	case chunkenc.ValFloatHistogram:
		newChunk = chunkenc.NewFloatHistogramChunk()
		if app, err = newChunk.Appender(); err != nil {
			break
		}

		for vt := valueType; vt != chunkenc.ValNone; vt = p.currDelIter.Next() {
			if vt != chunkenc.ValFloatHistogram {
				err = fmt.Errorf("found value type %v in histogram chunk", vt)
				break
			}
			var h *histogram.FloatHistogram
			t, h = p.currDelIter.AtFloatHistogram()
			_, _, app, err = app.AppendFloatHistogram(nil, t, h, true)
			if err != nil {
				break
			}
		}
	default:
		err = fmt.Errorf("populateWithDelChunkSeriesIterator: value type %v unsupported", valueType)
	}

	if err != nil {
		p.err = errors.Wrap(err, "iterate chunk while re-encoding")
		return false
	}
	if err := p.currDelIter.Err(); err != nil {
		p.err = errors.Wrap(err, "iterate chunk while re-encoding")
		return false
	}

	p.curr.Chunk = newChunk
	p.curr.MaxTime = t
	return true
}

func (p *populateWithDelChunkSeriesIterator) At() chunks.Meta { return p.curr }

// blockSeriesSet allows to iterate over sorted, populated series with applied tombstones.
// Series with all deleted chunks are still present as Series with no samples.
// Samples from chunks are also trimmed to requested min and max time.
type blockSeriesSet struct {
	blockBaseSeriesSet
}

func newBlockSeriesSet(i IndexReader, c ChunkReader, t tombstones.Reader, p index.Postings, mint, maxt int64, disableTrimming bool) storage.SeriesSet {
	return &blockSeriesSet{
		blockBaseSeriesSet{
			index:           i,
			chunks:          c,
			tombstones:      t,
			p:               p,
			mint:            mint,
			maxt:            maxt,
			disableTrimming: disableTrimming,
		},
	}
}

func (b *blockSeriesSet) At() storage.Series {
	// At can be looped over before iterating, so save the current values locally.
	return &blockSeriesEntry{
		chunks:     b.chunks,
		blockID:    b.blockID,
		seriesData: b.curr,
	}
}

// blockChunkSeriesSet allows to iterate over sorted, populated series with applied tombstones.
// Series with all deleted chunks are still present as Labelled iterator with no chunks.
// Chunks are also trimmed to requested [min and max] (keeping samples with min and max timestamps).
type blockChunkSeriesSet struct {
	blockBaseSeriesSet
}

func NewBlockChunkSeriesSet(id ulid.ULID, i IndexReader, c ChunkReader, t tombstones.Reader, p index.Postings, mint, maxt int64, disableTrimming bool) storage.ChunkSeriesSet {
	return &blockChunkSeriesSet{
		blockBaseSeriesSet{
			blockID:         id,
			index:           i,
			chunks:          c,
			tombstones:      t,
			p:               p,
			mint:            mint,
			maxt:            maxt,
			disableTrimming: disableTrimming,
		},
	}
}

func (b *blockChunkSeriesSet) At() storage.ChunkSeries {
	// At can be looped over before iterating, so save the current values locally.
	return &chunkSeriesEntry{
		chunks:     b.chunks,
		blockID:    b.blockID,
		seriesData: b.curr,
	}
}

// NewMergedStringIter returns string iterator that allows to merge symbols on demand and stream result.
func NewMergedStringIter(a, b index.StringIter) index.StringIter {
	return &mergedStringIter{a: a, b: b, aok: a.Next(), bok: b.Next()}
}

type mergedStringIter struct {
	a        index.StringIter
	b        index.StringIter
	aok, bok bool
	cur      string
	err      error
}

func (m *mergedStringIter) Next() bool {
	if (!m.aok && !m.bok) || (m.Err() != nil) {
		return false
	}
	switch {
	case !m.aok:
		m.cur = m.b.At()
		m.bok = m.b.Next()
		m.err = m.b.Err()
	case !m.bok:
		m.cur = m.a.At()
		m.aok = m.a.Next()
		m.err = m.a.Err()
	case m.b.At() > m.a.At():
		m.cur = m.a.At()
		m.aok = m.a.Next()
		m.err = m.a.Err()
	case m.a.At() > m.b.At():
		m.cur = m.b.At()
		m.bok = m.b.Next()
		m.err = m.b.Err()
	default: // Equal.
		m.cur = m.b.At()
		m.aok = m.a.Next()
		m.err = m.a.Err()
		m.bok = m.b.Next()
		if m.err == nil {
			m.err = m.b.Err()
		}
	}

	return true
}
func (m mergedStringIter) At() string { return m.cur }
func (m mergedStringIter) Err() error {
	return m.err
}

// DeletedIterator wraps chunk Iterator and makes sure any deleted metrics are not returned.
type DeletedIterator struct {
	// Iter is an Iterator to be wrapped.
	Iter chunkenc.Iterator
	// Intervals are the deletion intervals.
	Intervals tombstones.Intervals
}

func (it *DeletedIterator) At() (int64, float64) {
	return it.Iter.At()
}

func (it *DeletedIterator) AtHistogram() (int64, *histogram.Histogram) {
	t, h := it.Iter.AtHistogram()
	return t, h
}

func (it *DeletedIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	t, h := it.Iter.AtFloatHistogram()
	return t, h
}

func (it *DeletedIterator) AtT() int64 {
	return it.Iter.AtT()
}

func (it *DeletedIterator) Seek(t int64) chunkenc.ValueType {
	if it.Iter.Err() != nil {
		return chunkenc.ValNone
	}
	valueType := it.Iter.Seek(t)
	if valueType == chunkenc.ValNone {
		return chunkenc.ValNone
	}

	// Now double check if the entry falls into a deleted interval.
	ts := it.AtT()
	for _, itv := range it.Intervals {
		if ts < itv.Mint {
			return valueType
		}

		if ts > itv.Maxt {
			it.Intervals = it.Intervals[1:]
			continue
		}

		// We're in the middle of an interval, we can now call Next().
		return it.Next()
	}

	// The timestamp is greater than all the deleted intervals.
	return valueType
}

func (it *DeletedIterator) Next() chunkenc.ValueType {
Outer:
	for valueType := it.Iter.Next(); valueType != chunkenc.ValNone; valueType = it.Iter.Next() {
		ts := it.AtT()
		for _, tr := range it.Intervals {
			if tr.InBounds(ts) {
				continue Outer
			}

			if ts <= tr.Maxt {
				return valueType
			}
			it.Intervals = it.Intervals[1:]
		}
		return valueType
	}
	return chunkenc.ValNone
}

func (it *DeletedIterator) Err() error { return it.Iter.Err() }

type nopChunkReader struct {
	emptyChunk chunkenc.Chunk
}

func newNopChunkReader() ChunkReader {
	return nopChunkReader{
		emptyChunk: chunkenc.NewXORChunk(),
	}
}

func (cr nopChunkReader) Chunk(chunks.Meta) (chunkenc.Chunk, error) {
	return cr.emptyChunk, nil
}

func (cr nopChunkReader) Close() error { return nil }

type filteredSeriesSet struct {
	ms []*labels.Matcher
	s  storage.SeriesSet
}

func newFilteredSeriesSet(ms []*labels.Matcher, s storage.SeriesSet) *filteredSeriesSet {
	return &filteredSeriesSet{ms: ms, s: s}
}

func (f filteredSeriesSet) Next() bool {
	for f.s.Next() {
		if matchesMatcherSet(f.s.At().Labels(), f.ms) {
			return true
		}
	}
	return false
}

func (f filteredSeriesSet) At() storage.Series {
	return f.s.At()
}

func (f filteredSeriesSet) Err() error {
	return f.s.Err()
}

func (f filteredSeriesSet) Warnings() storage.Warnings {
	return f.s.Warnings()
}

type filteredChunkSeriesSet struct {
	ms []*labels.Matcher
	s  storage.ChunkSeriesSet
}

func newFilteredChunkSeriesSet(ms []*labels.Matcher, s storage.ChunkSeriesSet) *filteredChunkSeriesSet {
	return &filteredChunkSeriesSet{ms: ms, s: s}
}

func (f filteredChunkSeriesSet) Next() bool {
	for f.s.Next() {
		if matchesMatcherSet(f.s.At().Labels(), f.ms) {
			return true
		}
	}
	return false
}

func (f filteredChunkSeriesSet) At() storage.ChunkSeries {
	return f.s.At()
}

func (f filteredChunkSeriesSet) Err() error {
	return f.s.Err()
}

func (f filteredChunkSeriesSet) Warnings() storage.Warnings {
	return f.s.Warnings()
}
