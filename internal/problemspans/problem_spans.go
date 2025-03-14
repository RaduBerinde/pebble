// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package problemspans

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/google/btree"
)

type Set struct {
	cmp base.Compare

	sync.Mutex
	// tree is a btree of non-overlapping spans. Spans are ordered by level, then
	// by end key.
	tree *btree.BTreeG[span]
}

type span struct {
	level      int
	bounds     base.UserKeyBounds
	expiration time.Time
}

func (s span) Less(cmp base.Compare, other span) bool {
	if s.level != other.level {
		return s.level < other.level
	}
	return cmp(s.bounds.Start, other.bounds.Start) < 0
}

func New(cmp base.Compare) *Set {
	lessFn := func(a, b span) bool {
		if a.level != b.level {
			return a.level < b.level
		}
		return cmp(a.bounds.End.Key, b.bounds.End.Key) < 0
	}
	tree := btree.NewG[span](4, lessFn)
	return &Set{
		cmp:  cmp,
		tree: tree,
	}
}

// Add a span to the set. Any overlapping spans are split or removed as
// necessary (regardless of expiration).
func (s *Set) Add(level int, bounds base.UserKeyBounds, expiration time.Time) {
	s.Lock()
	defer s.Unlock()
	s.exciseLocked(level, bounds)
	s.tree.ReplaceOrInsert(span{
		level:      level,
		bounds:     bounds,
		expiration: expiration,
	})
}

func (s *Set) exciseLocked(level int, bounds base.UserKeyBounds) {
	// pivot is a dummy span that lets us start our search at the first span that
	// ends after Start.
	pivot := span{
		level:  level,
		bounds: base.UserKeyBoundsInclusive(bounds.Start, bounds.Start),
	}

	// Make a list of overlapping spans.
	var overlapping []span
	s.tree.DescendGreaterThan(pivot, func(sp span) bool {
		if sp.level != level || !bounds.End.IsUpperBoundFor(s.cmp, sp.bounds.Start) {
			return false
		}
		overlapping = append(overlapping, sp)
		return true
	})
	if len(overlapping) == 0 {
		return
	}

	// Remove all overlapping spans, then add back any segments that need to be
	// split at the start/end key.
	for _, sp := range overlapping {
		s.tree.Delete(sp)
	}
	first := overlapping[0]
	if s.cmp(first.bounds.Start, bounds.Start) < 0 {
		// The first span partially overlaps with our bounds; add back the portion
		// that is outside.
		first.bounds.End = base.UserKeyExclusive(bounds.Start)
		s.tree.ReplaceOrInsert(first)
	}
	// Note that first and last might be the same span; this works out well.
	last := overlapping[len(overlapping)-1]
	if bounds.End.CompareUpperBounds(s.cmp, last.bounds.End) < 0 {
		// The last span partially overlaps with our bounds; add back the portion
		// that is outside. Note that here the spans will "touch" if the end key
		// is inclusive, but that is ok.
		last.bounds.Start = bounds.End.Key
		s.tree.ReplaceOrInsert(last)
	}
}

// Excise removes any spans that overlap with the given bounds, splitting spans
// if necessary. Note that because start keys cannot be exclusive, it is
// possible that a span is left which starts at bounds.End.Key even when
// bounds.End is inclusive.
func (s *Set) Excise(level int, bounds base.UserKeyBounds) {
	s.Lock()
	defer s.Unlock()

	s.exciseLocked(level, bounds)
}

// Overlaps returns true if there is at least a span in the set that did not
// expire which overlaps the given bounds (for the level).
//
// Any spans in the range that have expired are removed.
func (s *Set) Overlaps(level int, bounds base.UserKeyBounds, now time.Time) bool {
	s.Lock()
	defer s.Unlock()
	if s.tree.Len() == 0 {
		// Fast path for the common case.
		return false
	}

	// pivot is a dummy span that lets us start our search for spans that start at
	// or after Start.
	pivot := span{
		level:  level,
		bounds: base.UserKeyBoundsInclusive(bounds.Start, bounds.Start),
	}

	overlap := false
	var toRemove []span
	s.tree.DescendGreaterThan(pivot, func(sp span) bool {
		if sp.level != level || !bounds.End.IsUpperBoundFor(s.cmp, sp.bounds.Start) {
			return false
		}
		if sp.expiration.Before(now) {
			toRemove = append(toRemove, sp)
		} else {
			overlap = true
		}
		return true
	})
	// Delete any expired spans we saw.
	for _, sp := range toRemove {
		s.tree.Delete(sp)
	}
	return overlap
}

func (s *Set) String() string {
	s.Lock()
	defer s.Unlock()

	var buf strings.Builder
	lastLevel := -1
	s.tree.Ascend(func(sp span) bool {
		if lastLevel != sp.level {
			fmt.Fprintf(&buf, "L%d:\n", sp.level)
			lastLevel = sp.level
		}
		fmt.Fprintf(&buf, "  %s expiration: %s\n", sp.bounds, sp.expiration)
		return true
	})
	return buf.String()
}
