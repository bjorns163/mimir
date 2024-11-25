// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/merge_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/prometheus/prometheus/model/histogram"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestMergeIter(t *testing.T) {
	for _, enc := range []chunk.Encoding{chunk.PrometheusXorChunk, chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
		t.Run(enc.String(), func(t *testing.T) {
			chunk1 := mkGenericChunk(t, 0, 100, enc)
			chunk2 := mkGenericChunk(t, model.TimeFromUnix(25), 100, enc)
			chunk3 := mkGenericChunk(t, model.TimeFromUnix(50), 100, enc)
			chunk4 := mkGenericChunk(t, model.TimeFromUnix(75), 100, enc)
			chunk5 := mkGenericChunk(t, model.TimeFromUnix(100), 100, enc)

			iter := NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testIter(t, 200, iter, enc, setNotCounterResetHintsAsUnknown)
			iter = NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testSeek(t, 200, iter, enc, setNotCounterResetHintsAsUnknown)

			// Re-use iterator.
			iter = NewGenericChunkMergeIterator(iter, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testIter(t, 200, iter, enc, setNotCounterResetHintsAsUnknown)
			iter = NewGenericChunkMergeIterator(iter, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testSeek(t, 200, iter, enc, setNotCounterResetHintsAsUnknown)
		})
	}
}

func TestMergeHarder(t *testing.T) {
	var (
		numChunks = 24 * 15
		chunks    = make([]GenericChunk, 0, numChunks)
		offset    = 30
		samples   = 100
	)
	for _, enc := range []chunk.Encoding{chunk.PrometheusXorChunk, chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
		t.Run(enc.String(), func(t *testing.T) {
			chunks = chunks[:0]
			from := model.Time(0)
			for i := 0; i < numChunks; i++ {
				chunks = append(chunks, mkGenericChunk(t, from, samples, enc))
				from = from.Add(time.Duration(offset) * time.Second)
			}
			iter := newMergeIterator(nil, chunks)
			testIter(t, offset*numChunks+samples-offset, newIteratorAdapter(nil, iter, labels.EmptyLabels()), enc, setNotCounterResetHintsAsUnknown)

			iter = newMergeIterator(nil, chunks)
			testSeek(t, offset*numChunks+samples-offset, newIteratorAdapter(nil, iter, labels.EmptyLabels()), enc, setNotCounterResetHintsAsUnknown)
		})
	}
}

type histSample struct {
	t    int64
	v    int
	hint histogram.CounterResetHint
}

func TestMergeHistogramCheckHints(t *testing.T) {
	for _, enc := range []chunk.Encoding{chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
		t.Run(enc.String(), func(t *testing.T) {
			for _, tc := range []struct {
				name            string
				chunks          []GenericChunk
				expectedSamples []histSample
			}{
				{
					name: "no overlapping iterators",
					chunks: []GenericChunk{
						mkGenericChunk(t, 0, 5, enc),
						mkGenericChunk(t, model.TimeFromUnix(5), 5, enc),
					},
					expectedSamples: []histSample{
						{t: 0, v: 0, hint: histogram.UnknownCounterReset},
						{t: 1000, v: 1000, hint: histogram.NotCounterReset},
						{t: 2000, v: 2000, hint: histogram.NotCounterReset},
						{t: 3000, v: 3000, hint: histogram.NotCounterReset},
						{t: 4000, v: 4000, hint: histogram.NotCounterReset},
						{t: 5000, v: 5000, hint: histogram.UnknownCounterReset},
						{t: 6000, v: 6000, hint: histogram.NotCounterReset},
						{t: 7000, v: 7000, hint: histogram.NotCounterReset},
						{t: 8000, v: 8000, hint: histogram.NotCounterReset},
						{t: 9000, v: 9000, hint: histogram.NotCounterReset},
					},
				},
				{
					name: "duplicated chunks",
					chunks: []GenericChunk{
						mkGenericChunk(t, 0, 10, enc),
						mkGenericChunk(t, 0, 10, enc),
					},
					expectedSamples: []histSample{
						{t: 0, v: 0, hint: histogram.UnknownCounterReset},       // 1 sample from c0
						{t: 1000, v: 1000, hint: histogram.UnknownCounterReset}, // 1 sample from c1
						{t: 2000, v: 2000, hint: histogram.UnknownCounterReset}, // 2 samples from c0
						{t: 3000, v: 3000, hint: histogram.NotCounterReset},
						{t: 4000, v: 4000, hint: histogram.UnknownCounterReset}, // 4 samples from c1
						{t: 5000, v: 5000, hint: histogram.NotCounterReset},
						{t: 6000, v: 6000, hint: histogram.NotCounterReset},
						{t: 7000, v: 7000, hint: histogram.NotCounterReset},
						{t: 8000, v: 8000, hint: histogram.UnknownCounterReset}, // 2 samples from c0
						{t: 9000, v: 9000, hint: histogram.NotCounterReset},
					},
				},
				//TODO: different sample values
			} {
				t.Run(tc.name, func(t *testing.T) {
					iter := NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), tc.chunks)
					for i, s := range tc.expectedSamples {
						valType := iter.Next()
						require.NotEqual(t, chunkenc.ValNone, valType)
						require.Nil(t, iter.Err())
						require.Equal(t, s.t, iter.AtT())
						switch enc {
						case chunk.PrometheusHistogramChunk:
							expH := test.GenerateTestHistogram(s.v)
							expH.CounterResetHint = s.hint
							_, actH := iter.AtHistogram(nil)
							test.RequireHistogramEqual(t, expH, actH, "expected sample %d does not match", i)
						case chunk.PrometheusFloatHistogramChunk:
							expH := test.GenerateTestFloatHistogram(s.v)
							expH.CounterResetHint = s.hint
							_, actH := iter.AtFloatHistogram(nil)
							test.RequireFloatHistogramEqual(t, expH, actH, "expected sample with idx %d does not match", i)
						default:
							t.Errorf("checkHints - internal error, unhandled expected type: %T", s)
						}
					}
					require.Equal(t, chunkenc.ValNone, iter.Next(), "iter has extra samples")
					require.Nil(t, iter.Err())
				})

			}
		})
	}
	//TODO: test seek should always return unknown
}

func testIterWithHints(t require.TestingT, points int, iter chunkenc.Iterator, encoding chunk.Encoding) {
	nextExpectedTS := model.TimeFromUnix(0)
	var assertPoint func(i int)
	switch encoding {
	case chunk.PrometheusHistogramChunk:
		assertPoint = func(i int) {
			require.Equal(t, chunkenc.ValHistogram, iter.Next(), strconv.Itoa(i))
			ts, h := iter.AtHistogram(nil)
			require.EqualValues(t, int64(nextExpectedTS), ts, strconv.Itoa(i))
			expH := test.GenerateTestHistogram(int(nextExpectedTS))
			test.RequireHistogramEqual(t, expH, h, strconv.Itoa(i))
			nextExpectedTS = nextExpectedTS.Add(step)
		}
	case chunk.PrometheusFloatHistogramChunk:
		assertPoint = func(i int) {
			require.Equal(t, chunkenc.ValFloatHistogram, iter.Next(), strconv.Itoa(i))
			ts, fh := iter.AtFloatHistogram(nil)
			require.EqualValues(t, int64(nextExpectedTS), ts, strconv.Itoa(i))
			expFH := test.GenerateTestFloatHistogram(int(nextExpectedTS))
			test.RequireFloatHistogramEqual(t, expFH, fh, strconv.Itoa(i))
			nextExpectedTS = nextExpectedTS.Add(step)
		}
	default:
		t.Errorf("testIterWithHints - unhandled encoding: %v", encoding)
	}
	for i := 0; i < points; i++ {
		assertPoint(i)
	}
	require.Equal(t, chunkenc.ValNone, iter.Next())
}

// TestMergeIteratorSeek tests a bug while calling Seek() on mergeIterator.
func TestMergeIteratorSeek(t *testing.T) {
	// Samples for 3 chunks.
	chunkSamples := [][]int64{
		{10, 20, 30, 40},
		{50, 60, 70, 80, 90, 100},
		{110, 120},
	}

	var genericChunks []GenericChunk
	for _, samples := range chunkSamples {
		ch := chunkenc.NewXORChunk()
		app, err := ch.Appender()
		require.NoError(t, err)
		for _, ts := range samples {
			app.Append(ts, 1)
		}

		genericChunks = append(genericChunks, NewGenericChunk(samples[0], samples[len(samples)-1], func(reuse chunk.Iterator) chunk.Iterator {
			chk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
			require.NoError(t, err)
			require.NoError(t, chk.UnmarshalFromBuf(ch.Bytes()))
			return chk.NewIterator(reuse)
		}))
	}

	c3It := NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), genericChunks)

	c3It.Seek(15)
	// These Next() calls are necessary to reproduce the bug.
	c3It.Next()
	c3It.Next()
	c3It.Next()
	c3It.Seek(75)
	// Without the bug fix, this Seek() call skips an additional sample than desired.
	// Instead of stopping at 100, which is the last sample of chunk 2,
	// it would go to 110, which is the first sample of chunk 3.
	// This was happening because in the Seek() method we were discarding the current
	// batch from mergeIterator if the batch's first sample was after the seek time.
	c3It.Seek(95)

	require.Equal(t, int64(100), c3It.AtT())
}
