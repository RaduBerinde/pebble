// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeletionPacer(t *testing.T) {
	const MB = 1 << 20
	const GB = 1 << 30
	testCases := []struct {
		freeBytes     uint64
		obsoleteBytes uint64
		liveBytes     uint64
		// history of deletion reporting; first value in the pair is the time,
		// second value is the deleted bytes. The time of pacing is the same as the
		// last time in the history.
		history [][2]int
		// expected wait time for 100 MB.
		expected float64
	}{
		{
			freeBytes:     160 * GB,
			obsoleteBytes: 1 * MB,
			liveBytes:     160 * MB,
			expected:      1.0,
		},
		// As freeBytes < free space threshold, there should be no throttling.
		{
			freeBytes:     5 * GB,
			obsoleteBytes: 1 * MB,
			liveBytes:     100 * MB,
			expected:      0.0,
		},
		// As obsoleteBytesRatio > 0.20, there should be no throttling.
		{
			freeBytes:     500 * GB,
			obsoleteBytes: 50 * MB,
			liveBytes:     100 * MB,
			expected:      0.0,
		},
		// When obsolete ratio unknown, there should be no throttling.
		{
			freeBytes:     500 * GB,
			obsoleteBytes: 0,
			liveBytes:     0,
			expected:      0.0,
		},
		// History shows 200MB/sec deletions on average over last 5 minutes, wait
		// time should be half.
		{
			freeBytes:     160 * GB,
			obsoleteBytes: 1 * MB,
			liveBytes:     160 * MB,
			history:       [][2]int{{0, 5 * 60 * 200 * MB}},
			expected:      0.5,
		},
		{
			freeBytes:     160 * GB,
			obsoleteBytes: 1 * MB,
			liveBytes:     160 * MB,
			history:       [][2]int{{0, 60 * 1000 * MB}, {3 * 60, 60 * 4 * 1000 * MB}, {4 * 60, 0}},
			expected:      0.1,
		},
		// First entry in history is too old, it should be discarded.
		{
			freeBytes:     160 * GB,
			obsoleteBytes: 1 * MB,
			liveBytes:     160 * MB,
			history:       [][2]int{{0, 10 * 60 * 10000 * MB}, {3 * 60, 4 * 60 * 200 * MB}, {7 * 60, 1 * 60 * 200 * MB}},
			expected:      0.5,
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("%d", tcIdx), func(t *testing.T) {
			getInfo := func() deletionPacerInfo {
				return deletionPacerInfo{
					freeBytes:     tc.freeBytes,
					liveBytes:     tc.liveBytes,
					obsoleteBytes: tc.obsoleteBytes,
				}
			}
			start := time.Now()
			last := start
			pacer := newDeletionPacer(start, 100*MB, getInfo)
			for _, h := range tc.history {
				last = start.Add(time.Second * time.Duration(h[0]))
				pacer.ReportDeletion(last, uint64(h[1]))
			}
			result := pacer.PacingDelay(last, 100*MB)
			require.InDelta(t, tc.expected, result, 1e-7)
		})
	}
}

// TestDeletionPacerHistory tests the history helper by crosschecking Sum()
// against a naive implementation.
func TestDeletionPacerHistory(t *testing.T) {
	type event struct {
		time time.Time
		// If report is 0, this event is a Sum(). Otherwise it is an Add().
		report int64
	}
	numEvents := 1 + rand.Intn(200)
	timeframeSecs := 1 + rand.Intn(60*100)
	events := make([]event, numEvents)
	startTime := time.Now()
	for i := range events {
		events[i].time = startTime.Add(time.Duration(rand.Intn(timeframeSecs * int(time.Second))))
		if rand.Intn(3) == 0 {
			events[i].report = 0
		} else {
			events[i].report = int64(rand.Intn(100000))
		}
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].time.Before(events[j].time)
	})

	windowSecs := 1 + rand.Intn(timeframeSecs)
	var h history
	h.Init(startTime, windowSecs)

	for i, e := range events {
		if e.report != 0 {
			h.Add(e.time, e.report)
			continue
		}
		// Sum all report values in the last windowSecs.
		eSec := int(e.time.Sub(startTime) / time.Second)
		var sum int64
		for j := i - 1; j >= 0; j-- {
			if jSec := int(events[j].time.Sub(startTime) / time.Second); jSec <= eSec-windowSecs {
				break
			}
			sum += events[j].report
		}
		actual := h.Sum(e.time)
		require.Equal(t, sum, actual)
	}
}
