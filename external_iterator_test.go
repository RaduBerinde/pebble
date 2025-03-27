// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestExternalIterator(t *testing.T) {
	mem := vfs.NewMem()
	o := &Options{
		FS:                 mem,
		FormatMajorVersion: internalFormatNewest,
		Comparer:           testkeys.Comparer,
	}
	o.Experimental.EnableColumnarBlocks = func() bool { return true }
	o.testingRandomized(t)
	o.EnsureDefaults()
	d, err := Open("", o)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	datadriven.RunTest(t, "testdata/external_iterator", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			mem = vfs.NewMem()
			return ""
		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""
		case "iter":
			opts := IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
			var files [][]sstable.ReadableFile
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "mask-suffix":
					opts.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				case "files":
					for _, v := range arg.Vals {
						f, err := mem.Open(v)
						require.NoError(t, err)
						files = append(files, []sstable.ReadableFile{f})
					}
				}
			}
			testExternalIteratorInitError(t, o, &opts, files)
			it, err := NewExternalIter(o, &opts, files)
			require.NoError(t, err)
			return runIterCmd(td, it, true /* close iter */)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// testExternalIteratorInitError tests error handling paths inside
// NewExternalIter by injecting errors when reading files.
//
// See github.com/cockroachdb/cockroach/issues/141606 where an error during
// initialization caused NewExternalIter to panic.
func testExternalIteratorInitError(
	t *testing.T, o *Options, iterOpts *IterOptions, files [][]sstable.ReadableFile,
) {
	files = slices.Clone(files)
	for i := range files {
		files[i] = slices.Clone(files[i])
		for j := range files[i] {
			files[i][j] = &flakyFile{ReadableFile: files[i][j]}
		}
	}

	for iter := 0; iter < 100; iter++ {
		it, err := NewExternalIter(o, iterOpts, files)
		if err != nil {
			require.Contains(t, err.Error(), "flaky file")
		} else {
			it.Close()
		}
	}
}

type flakyFile struct {
	sstable.ReadableFile
}

func (ff *flakyFile) ReadAt(p []byte, off int64) (n int, err error) {
	if rand.IntN(10) == 0 {
		return 0, errors.New("flaky file")
	}
	return ff.ReadableFile.ReadAt(p, off)
}

func (ff *flakyFile) Close() error { return nil }
