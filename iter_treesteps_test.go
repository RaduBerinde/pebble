// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build invariants

package pebble

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/invalidating"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/treesteps"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestIterTreeSteps tests the treesteps recording for various iterator types,
// generating visualization URLs showing iterator behavior.
func TestIterTreeSteps(t *testing.T) {
	if !treesteps.Enabled {
		t.Skip("treesteps not available in this build")
	}

	t.Run("levelIter", func(t *testing.T) {
		testIterTreeSteps(t, "testdata/iter_treesteps_level_iter")
	})
}

func testIterTreeSteps(t *testing.T, testdataPath string) {
	invalidating.Disable()
	defer invalidating.Enable()
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
			d = nil
		}
	}()

	datadriven.RunTest(t, testdataPath, func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			if d != nil {
				require.NoError(t, d.Close())
				d = nil
			}
			opts := &Options{
				Comparer: testkeys.Comparer,
				FS:       vfs.NewMem(),
			}
			var err error
			d, err = runDBDefineCmd(td, opts)
			require.NoError(t, err)
			return ""

		case "iter-treesteps":
			return runIterTreeStepsCmd(t, d, td)

		default:
			return "unknown command"
		}
	})
}

func runIterTreeStepsCmd(t *testing.T, d *DB, td *datadriven.TestData) string {
	require.NotNil(t, d, "must run 'define' before 'iter-treesteps'")

	iter, _ := d.NewIter(nil)
	defer iter.Close()
	var opts []treesteps.RecordingOption
	var depth int
	td.MaybeScanArgs(t, "depth", &depth)
	if depth != 0 {
		opts = append(opts, treesteps.MaxTreeDepth(depth))
		opts = append(opts, treesteps.MaxOpDepth(depth))
	}
	rec := treesteps.StartRecording(iter, td.Pos, opts...)
	out := runIterCmd(td, iter, true /* closeIter */)
	fmt.Println(out)

	steps := rec.Finish()
	url := steps.URL()
	return url.String()
}
