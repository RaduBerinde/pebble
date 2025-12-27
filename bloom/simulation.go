// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"

	"github.com/cockroachdb/crlib/crhumanize"
	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/pebble/internal/metricsutil"
)

func SimulateFPR(bitsPerKey int, numProbes int, twoBlocks bool) (float64, string) {
	const size = 10_000
	const numRuns = 1000

	var wg sync.WaitGroup
	ch := make(chan struct{}, numRuns)
	for range numRuns {
		ch <- struct{}{}
	}
	close(ch)

	var fprMu sync.Mutex
	var fpr metricsutil.Welford

	for range runtime.GOMAXPROCS(0) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range ch {
				numHashes := size - size/10 + rand.IntN(size*2/10)
				hc := &hashCollector{}
				hc.Init()
				for range numHashes {
					hc.Add(rand.Uint32())
				}
				nLines := calculateNumLines(hc.NumHashes(), uint32(bitsPerKey))
				var filter []byte
				if twoBlocks {
					filter = buildTwoBlocksFilter(nLines, uint32(numProbes), hc)
				} else {
					filter = buildFilter(nLines, uint32(numProbes), hc)
				}
				hc.Reset()

				queries := cacheLineSize * numHashes
				positives := 0
				bits := aliasFilterBits(filter, uint32(len(filter)-5)/cacheLineSize)
				for range queries {
					h := rand.Uint32()
					if bits.probe(uint8(numProbes), h) {
						if !twoBlocks || bits.probe(uint8(numProbes), remix32(h)) {
							positives++
						}
					}
				}
				positiveRate := float64(positives) / float64(queries)
				truePositiveRate := float64(numHashes) / float64(1<<32)
				falsePositiveRate := positiveRate - truePositiveRate
				fprMu.Lock()
				fpr.Add(falsePositiveRate)
				fprMu.Unlock()
			}
		}()
	}
	wg.Wait()

	mean := fpr.Mean()
	fmt.Printf(
		"%d bits per key, %s%d probes: FPR %s ± %s\n",
		bitsPerKey,
		crstrings.If(twoBlocks, "2x"), numProbes,
		formatFPR(mean), crhumanize.Percent(fpr.StdDev(), mean),
	)
	return mean, fmt.Sprintf("%s ± %s", formatFPR(mean), crhumanize.Percent(fpr.StdDev(), mean))
}

// formatFPR formats a false positive rate as a percentage with "1 in N" ratio.
func formatFPR(fpr float64) string {
	l10 := min(3, -int(math.Floor(math.Log10(fpr))))
	return fmt.Sprintf("%.*f%% (1 in %.*f)", l10, fpr*100, 3-l10, 1.0/fpr)
}
