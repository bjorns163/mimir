// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func main() {
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Println("Failed to parse CLI arguments:", err.Error())
		os.Exit(1)
	}

	for _, arg := range args {
		resps, err := parseFile(arg)
		if err != nil {
			fmt.Println("Failed to parse file:", err.Error())
			os.Exit(1)
		}

		for _, res := range resps {
			dumpResponse(res)
		}
	}
}

func parseFile(file string) ([]QueryStreamResponse, error) {
	resps := []QueryStreamResponse{}

	fileData, err := os.ReadFile(file) //#nosec G109 -- this is intentionally taking operator input, not an injection.
	if err != nil {
		return nil, err
	}

	// Decode file.
	decoder := json.NewDecoder(bytes.NewReader(fileData))
	for {
		res := QueryStreamResponse{}
		if err := decoder.Decode(&res); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		resps = append(resps, res)
	}

	return resps, nil
}

func dumpResponse(res QueryStreamResponse) {
	slices.SortFunc(res.Chunkseries, func(a, b QueryStreamChunkseries) int {
		return labels.Compare(a.LabelSet(), b.LabelSet())
	})

	for _, series := range res.Chunkseries {
		fmt.Println(series.LabelSet().String())
		var (
			h  *histogram.Histogram
			fh *histogram.FloatHistogram
			ts int64
		)

		slices.SortFunc(series.Chunks, func(a, b Chunk) int {
			return int(a.StartTimestamp() - b.StartTimestamp())
		})

		for _, chunk := range series.Chunks {
			fmt.Printf(
				"- Chunk: %s - %s\n",
				chunk.StartTime().Format(time.RFC3339),
				chunk.EndTime().Format(time.RFC3339))

			chunkIterator := chunk.EncodedChunk().NewIterator(nil)
			for {
				sampleType := chunkIterator.Scan()
				if sampleType == chunkenc.ValNone {
					break
				}

				switch sampleType {
				case chunkenc.ValFloat:
					fmt.Println("  - Sample:", sampleType.String(), "ts:", chunkIterator.Timestamp(), "value:", chunkIterator.Value().Value)
				case chunkenc.ValHistogram:
					ts, h = chunkIterator.AtHistogram(h)
					fmt.Println("  - Sample:", sampleType.String(), "ts:", ts, "value:", h, "hint:", counterResetHintString(h.CounterResetHint))
				case chunkenc.ValFloatHistogram:
					ts, fh := chunkIterator.AtFloatHistogram(fh)
					fmt.Println("  - Sample:", sampleType.String(), "ts:", ts, "value:", fh, "hint:", counterResetHintString(fh.CounterResetHint))
				default:
					panic(fmt.Errorf("unknown sample type %s", sampleType.String()))
				}
			}

			if chunkIterator.Err() != nil {
				panic(chunkIterator.Err())
			}
		}
	}
}

func counterResetHintString(crh histogram.CounterResetHint) string {
	switch crh {
	case histogram.UnknownCounterReset:
		return "UnknownCounterReset"
	case histogram.CounterReset:
		return "CounterReset"
	case histogram.NotCounterReset:
		return "NotCounterReset"
	case histogram.GaugeType:
		return "GaugeType"
	default:
		return "unrecognized counter reset hint"
	}
}
