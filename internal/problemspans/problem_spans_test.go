// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package problemspans

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
)

var baseTime = time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)

func TestDataDriven(t *testing.T) {
	set := New(base.DefaultComparer.Compare)

	datadriven.RunTest(t, "testdata/problem_spans", func(t *testing.T, td *datadriven.TestData) string {
		var out bytes.Buffer
		switch td.Cmd {
		case "init":
			set = New(base.DefaultComparer.Compare)

		case "add":
			for _, line := range crstrings.Lines(td.Input) {
				level, bounds, expiration := parseLine(line, true /* withTime */)
				exp := baseTime.Add(time.Duration(expiration) * time.Minute)
				set.Add(level, bounds, exp)
			}

		case "excise":
			for _, line := range crstrings.Lines(td.Input) {
				level, bounds, _ := parseLine(line, false /* withTime */)
				set.Excise(level, bounds)
			}

		case "overlap":
			for _, line := range crstrings.Lines(td.Input) {
				level, bounds, now := parseLine(line, true /* withTime */)
				nowTime := baseTime.Add(time.Duration(now) * time.Minute)
				res := "overlap"
				if !set.Overlaps(level, bounds, nowTime) {
					res = "no overlap"
				}
				fmt.Fprintf(&out, "L%d %s: %s\n", level, bounds, res)
			}
		default:
			td.Fatalf(t, "unknown command %q", td.Cmd)
		}
		setStr := set.String()
		// Make the times look friendlier, since we're only dealing with minutes.
		setStr = strings.ReplaceAll(setStr, "2025-01-01 00:", "")
		setStr = strings.ReplaceAll(setStr, ":00 +0000 UTC", "m")
		out.WriteString(setStr)
		return out.String()
	})
}

func parseLine(line string, withTime bool) (level int, _ base.UserKeyBounds, minutes int) {
	var span1, span2 string
	var n int
	var err error
	if withTime {
		n, err = fmt.Sscanf(line, "L%d %s %s %dm", &level, &span1, &span2, &minutes)
	} else {
		n, err = fmt.Sscanf(line, "L%d %s %s", &level, &span1, &span2)
	}
	if err != nil || n != 4 {
		panic(fmt.Sprintf("error parsing line %q: n=%d err=%v", line, n, err))
	}
	bounds := base.ParseUserKeyBounds(span1 + " " + span2)
	return level, bounds, minutes
}
