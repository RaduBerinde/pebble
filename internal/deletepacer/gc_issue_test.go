// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import (
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
)

func TestFoo(t *testing.T) {
	for i := 0; i < 1000; i++ {
		runOne(t)
	}
}

func runOne(t *testing.T) {
	opts := Options{
		FreeSpaceTimeframe: 10 * time.Second,
		BacklogTimeframe:   50 * time.Minute,
		BaselineRate:       func() uint64 { return 1000000 },
	}
	deleteFn := func(of ObsoleteFile, jobID int) {
	}
	dp := Open(opts, testutils.Logger{T: t}, func() uint64 { return 1 << 30 }, deleteFn)
	defer dp.Close()

	var wg sync.WaitGroup
	defer wg.Wait()
	n := 1 + rand.Intn(4)
	for j := 0; j < n; j++ {
		rng := rand.New(rand.NewSource(rand.Int63()))
		wg.Add(1)
		go func(enqueue bool) {
			defer wg.Done()
			if enqueue {
				time.Sleep(time.Duration(rng.Intn(10_000)))
				dp.Enqueue(1, ObsoleteFile{
					FileType: base.FileTypeTable,
					Path:     fmt.Sprintf("this is some string %d", rng.Intn(1000)),
					FileSize: 1000,
					IsLocal:  false,
				})
			}
			a := make([]byte, 100)
			for k := 0; k < 100; k++ {
				for x := range a[rng.Intn(len(a))] {
					a[x] = byte(rng.Intn(256))
				}
				fmt.Fprintf(io.Discard, "%v\n", a)
			}
		}(j == n-1)
	}
}
