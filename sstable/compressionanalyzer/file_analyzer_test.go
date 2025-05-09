// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compressionanalyzer

import (
	"context"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/compression"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func TestFileAnalyzer(t *testing.T) {
	datadriven.RunTest(t, "testdata/file_analyzer", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "sst":
			fa := NewFileAnalyzer(nil, sstable.ReaderOptions{})
			defer fa.Close()
			for _, path := range crstrings.Lines(td.Input) {
				if err := fa.SSTable(context.Background(), vfs.Default, path); err != nil {
					td.Fatalf(t, "%v", err)
				}
			}
			// Clear out values that are not reliable.
			for i := range fa.b {
				for j := range fa.b[i] {
					for k := range fa.b[i][j] {
						bucket := &fa.b[i][j][k]
						for l := range bucket.Experiments {
							// Snappy always has the same output in all configurations and on
							// all platforms.
							if Settings[l].Algorithm != compression.SnappyAlgorithm {
								bucket.Experiments[l].CompressionRatio = Welford{}
							}
							bucket.Experiments[l].CompressionTime = Welford{}
							bucket.Experiments[l].DecompressionTime = Welford{}
						}
					}
				}
			}
			return fa.b.String(1)

		default:
			td.Fatalf(t, "unknown command %s", td.Cmd)
			return ""
		}
	})
}
