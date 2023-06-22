// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeletionPacerMaybeThrottle(t *testing.T) {
	const MB = 1 << 20
	const GB = 1 << 30
	testCases := []struct {
		freeBytes     uint64
		obsoleteBytes uint64
		liveBytes     uint64
		// expected scaling of wait time.
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
		// obsoleteBytesRatio is mid-way between 5% and 50%, the scale should be 0.5.
		{
			freeBytes:     500 * GB,
			obsoleteBytes: 125 * MB,
			liveBytes:     1000 * MB,
			expected:      0.5,
		},
		// obsoleteBytes is 10GB, there should be no throttling.
		{
			freeBytes:     500 * GB,
			obsoleteBytes: 10 * GB,
			liveBytes:     5000 * GB,
			expected:      0.0,
		},
		// obsoleteBytes is 5.5GB, scale should be 0.5.
		{
			freeBytes:     500 * GB,
			obsoleteBytes: 11 * GB / 2,
			liveBytes:     5000 * GB,
			expected:      0.5,
		},
		// When obsolete ratio unknown, there should be no throttling.
		{
			freeBytes:     500 * GB,
			obsoleteBytes: 0,
			liveBytes:     0,
			expected:      0.0,
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
			pacer := newDeletionPacer(1, getInfo)
			result := pacer.PacingDelay(1)
			require.InDelta(t, tc.expected, result, 1e-7)
		})
	}
}
