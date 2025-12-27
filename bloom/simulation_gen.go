// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build ignore

// This program generates the simulation.md file via:
//
//	go run simulation_gen.go
package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/ascii"
)

func main() {
	const maxBitsPerKey = 20
	const maxProbes = 16

	var fpr [2][maxBitsPerKey + 1][maxProbes + 1]float64
	var fprStr [2][maxBitsPerKey + 1][maxProbes + 1]string
	var best [2][maxBitsPerKey + 1]int

	for twoBlocks := range 2 {
		for bpk := 1; bpk <= maxBitsPerKey; bpk++ {
			for p := 1; p <= bpk && p <= maxProbes; p++ {
				fpr[twoBlocks][bpk][p], fprStr[twoBlocks][bpk][p] = bloom.SimulateFPR(bpk, p, twoBlocks == 1)
				if best[twoBlocks][bpk] == 0 || fpr[twoBlocks][bpk][p] < fpr[twoBlocks][bpk][best[twoBlocks][bpk]] {
					best[twoBlocks][bpk] = p
				}
			}
			// Pick the smallest number of probes that has FPR within 1% of the optimal.
			for i := 1; i < best[twoBlocks][bpk]; i++ {
				if fpr[twoBlocks][bpk][i] < fpr[twoBlocks][bpk][best[twoBlocks][bpk]]*1.01 {
					best[twoBlocks][bpk] = i
					break
				}
			}
		}
	}
	blockStr := [2]string{"Single block", "Two blocks"}
	probeStr := func(twoBlocks int, p int) string {
		if twoBlocks == 0 {
			return fmt.Sprintf(" %2d ", p)
		}
		return fmt.Sprintf("%2dx2", p)
	}

	board := ascii.Make(100, 100)
	cur := board.At(0, 0)
	cur = cur.WriteString("# Bloom filter simulation results\n\n")

	for twoBlocks := range 2 {
		cur = cur.Printf("## %s\n\n", blockStr[twoBlocks])

		cur = cur.WriteString("| Bits/key | Probes |            FPR            |\n")
		cur = cur.WriteString("|---------:|:------:|:-------------------------:|\n")
		for bpk := 1; bpk <= maxBitsPerKey; bpk++ {
			p := best[twoBlocks][bpk]
			cur = cur.Printf("| %8d |  %s  | %-25s |\n", bpk, probeStr(twoBlocks, p), fprStr[twoBlocks][bpk][p])
		}
		cur = cur.NewlineReturn()
	}

	cur = cur.WriteString("\n## Full data\n")
	for twoBlocks := range 2 {
		cur = cur.Printf("\n### %s\n\n", blockStr[twoBlocks])

		cur = cur.WriteString("| Bits/key | Probes |            FPR            |\n")
		cur = cur.WriteString("|---------:|:------:|:-------------------------:|\n")

		for bpk := 1; bpk <= maxBitsPerKey; bpk++ {
			if bpk > 1 {
				cur = cur.WriteString("|          |        |                           |\n")
			}
			for p := 1; p <= bpk && p <= maxProbes; p++ {
				cur = cur.Printf("| %8d |  %s  | %-25s |\n", bpk, probeStr(twoBlocks, p), fprStr[twoBlocks][bpk][p])
			}
		}
	}
	fmt.Println(board.String())
	err := os.WriteFile("simulation.md", []byte(board.String()+"\n"), 0644)
	if err != nil {
		panic(err)
	}
}
