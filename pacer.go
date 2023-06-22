// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"
	"time"
)

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
	freeSpaceThreshold uint64

	// If the ratio of obsolete bytes to live bytes is greater than
	// obsoleteBytesMaxRatio, do not pace deletions at all.
	obsoleteBytesMaxRatio float64

	mu struct {
		sync.Mutex

		// history keeps rack of recent deletion history; it used to increase the
		// deletion rate to match the pace of deletions.
		history history
	}

	targetByteDeletionRate int64

	getInfo func() deletionPacerInfo
}

const deletePacerHistorySeconds = 5 * 60 // 5 minutes

// newDeletionPacer instantiates a new deletionPacer for use when deleting
// obsolete files.
//
// targetByteDeletionRate is the rate (in bytes/sec) at which we want to
// normally limit deletes (when we are not falling behind or running out of
// space). A value of 0.0 disables pacing.
func newDeletionPacer(
	now time.Time, targetByteDeletionRate int64, getInfo func() deletionPacerInfo,
) *deletionPacer {
	d := &deletionPacer{
		freeSpaceThreshold:    16 << 30, // 16 GB
		obsoleteBytesMaxRatio: 0.20,

		targetByteDeletionRate: targetByteDeletionRate,
		getInfo:                getInfo,
	}
	d.mu.history.Init(now, deletePacerHistorySeconds)
	return d
}

// ReportDeletion is used to report a deletion to the pacer. The pacer uses it
// to keep track of the recent rate of deletions and potentially increase the
// deletion rate accordingly.
//
// ReportDeletion is thread-safe.
func (p *deletionPacer) ReportDeletion(now time.Time, bytesToDelete uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.history.Add(now, int64(bytesToDelete))
}

// PacingDelay returns the recommended pacing wait time (in seconds) for
// deleting the given number of bytes.
//
// PacingDelay is thread-safe.
func (p *deletionPacer) PacingDelay(now time.Time, bytesToDelete uint64) (waitSeconds float64) {
	if p.targetByteDeletionRate == 0 {
		// Pacing disabled.
		return 0.0
	}

	info := p.getInfo()
	if info.freeBytes <= p.freeSpaceThreshold {
		return 0.0
	}
	obsoleteBytesRatio := 1.0
	if info.liveBytes > 0 {
		obsoleteBytesRatio = float64(info.obsoleteBytes) / float64(info.liveBytes)
	}
	if obsoleteBytesRatio >= p.obsoleteBytesMaxRatio {
		return 0.0
	}

	rate := p.targetByteDeletionRate

	// See if recent deletion rate is more than our target; if so, use that as our
	// target so that we don't fall behind.
	p.mu.Lock()
	defer p.mu.Unlock()
	if historyRate := p.mu.history.Sum(now) / deletePacerHistorySeconds; rate < historyRate {
		rate = historyRate
	}

	return float64(bytesToDelete) / float64(rate)
}

// history is a helper used to keep track of the recent history of a set of
// data points (in our case deleted bytes), at 1 second granularity.
type history struct {
	startTime time.Time
	// currSec is the second (since startTime) of the most recent operation.
	currSec int
	// val contains the recent values, at second granularity.
	// val[currSec % len(val)] is the current second.
	// val[(currSec + 1) % len(val)] is the oldest second.
	val []int64
	// sum is always equal to the sum of values in val.
	sum int64
}

// Init the history helper to keep track of data over the given number of
// seconds.
func (h *history) Init(now time.Time, numSeconds int) {
	*h = history{
		startTime: now,
		currSec:   0,
		val:       make([]int64, numSeconds),
		sum:       0,
	}
}

// Add adds a value for the current second.
func (h *history) Add(now time.Time, val int64) {
	h.advance(now)
	h.val[h.currSec%len(h.val)] += val
	h.sum += val
}

// Sum returns the sum of recent values.
func (h *history) Sum(now time.Time) int64 {
	h.advance(now)
	return h.sum
}

// advance advances the time to the given time.
func (h *history) advance(now time.Time) {
	sec := int(now.Sub(h.startTime) / time.Second)
	for h.currSec < sec {
		h.currSec++
		// Forget the data for the oldest second.
		h.sum -= h.val[h.currSec%len(h.val)]
		h.val[h.currSec%len(h.val)] = 0
	}
}
