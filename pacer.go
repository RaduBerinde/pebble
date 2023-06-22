// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

// deletionPacerInfo contains any info from the db necessary to make deletion
// pacing decisions (to limit background IO usage so that it does not contend
// with foreground traffic).
type deletionPacerInfo struct {
	freeBytes     uint64
	obsoleteBytes uint64
	liveBytes     uint64
}

// deletionPacer rate limits deletions of obsolete files. This is necessary to
// prevent overloading the disk with too many deletions too quickly after a
// large compaction, or an iterator close. On some SSDs, disk performance can be
// negatively impacted if too many blocks are deleted very quickly, so this
// mechanism helps mitigate that.
type deletionPacer struct {
	// If there are less than freeSpaceThreshold bytes of free space on
	// disk, do not pace deletions at all.
	freeSpaceScaler linearScaler

	// If the ratio of obsolete bytes to live bytes is greater than
	// obsoleteBytesMaxRatio, do not pace deletions at all.
	obsoleteBytesRatioScaler    linearScaler
	obsoleteBytesAbsoluteScaler linearScaler

	targetByteDeletionRate float64

	getInfo func() deletionPacerInfo
}

// newDeletionPacer instantiates a new deletionPacer for use when deleting
// obsolete files.
//
// targetByteDeletionRate is the rate (in bytes/sec) at which we want to
// normally limit deletes (when we are not falling behind or running out of
// space). A value of 0.0 disables pacing.
func newDeletionPacer(targetByteDeletionRate int, getInfo func() deletionPacerInfo) *deletionPacer {
	const GB = 1 << 30
	const percent = 0.01
	// TODO(radu): these thresholds should be configurable.
	return &deletionPacer{
		// We start increasing the deletion rate when free space falls below 32B; we
		// completely disable pacing when it falls below 16GB.
		freeSpaceScaler: makeLinearScaler(16*GB, 32*GB),

		// We start increasing the deletion rate when obsolete bytes to live bytes
		// ratio goes above 5%; we completely disable pacing when it gets to 20%.
		obsoleteBytesRatioScaler: makeLinearScaler(5*percent, 20*percent),

		// We start increasing the deletion rate when we have 1GB obsolete bytes; we
		// completely disable pacing when we have 10GB obsolete bytes.
		obsoleteBytesAbsoluteScaler: makeLinearScaler(1*GB, 10*GB),

		targetByteDeletionRate: float64(targetByteDeletionRate),

		getInfo: getInfo,
	}
}

// PacingDelay returns the recommended pacing wait time (in seconds) for
// deleting the given number of bytes.
func (p *deletionPacer) PacingDelay(bytesToDelete uint64) (waitSeconds float64) {
	if p.targetByteDeletionRate == 0.0 {
		// Pacing disabled.
		return 0
	}

	info := p.getInfo()
	// The scaling is opposite for free space (high value should mean scale 1.0).
	scale := 1.0 - p.freeSpaceScaler.scale(float64(info.freeBytes))

	if s := p.obsoleteBytesAbsoluteScaler.scale(float64(info.obsoleteBytes)); s < scale {
		scale = s
	}

	obsoleteBytesRatio := 1.0
	if info.liveBytes > 0 {
		obsoleteBytesRatio = float64(info.obsoleteBytes) / float64(info.liveBytes)
	}
	if s := p.obsoleteBytesRatioScaler.scale(obsoleteBytesRatio); s < scale {
		scale = s
	}
	return scale * float64(bytesToDelete) / p.targetByteDeletionRate
}

// linearScaler is a helper for calculating a scaling factor based on a value
// that we want to keep below certain targets.
//
// Specifically, we have a low and a high threshold; the scaling factor is 1.0
// whenever the value is under the low threshold. Above that, the scaling factor
// decreases linearly until it reaches 0.0 at the high threshold.
type linearScaler struct {
	lowThreshold  float64
	highThreshold float64
}

func makeLinearScaler(lowThreshold, highThreshold float64) linearScaler {
	return linearScaler{
		lowThreshold:  lowThreshold,
		highThreshold: highThreshold,
	}
}

func (s linearScaler) scale(value float64) float64 {
	switch {
	case value <= s.lowThreshold:
		return 1.0
	case value >= s.highThreshold:
		return 0.0
	default:
		return (s.highThreshold - value) / (s.highThreshold - s.lowThreshold)
	}
}
