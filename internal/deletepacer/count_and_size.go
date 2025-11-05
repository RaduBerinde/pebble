// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import "fmt"

// CountAndSize tracks the count and total size of a set of items.
type CountAndSize struct {
	// Count is the number of files.
	Count uint64

	// Bytes is the total size of all files.
	Bytes uint64
}

// Inc increases the count and size for a single item.
func (cs *CountAndSize) Inc(fileSize uint64) {
	cs.Count++
	cs.Bytes += fileSize
}

// Dec decreases the count and size for a single item.
func (cs *CountAndSize) Dec(fileSize uint64) {
	cs.Count = SafeSub(cs.Count, 1)
	cs.Bytes = SafeSub(cs.Bytes, fileSize)
}

// Accumulate increases the counts and sizes by the given amounts.
func (cs *CountAndSize) Accumulate(other CountAndSize) {
	cs.Count += other.Count
	cs.Bytes += other.Bytes
}

// Deduct decreases the counts and sizes by the given amounts.
func (cs *CountAndSize) Deduct(other CountAndSize) {
	cs.Count = SafeSub(cs.Count, other.Count)
	cs.Bytes = SafeSub(cs.Bytes, other.Bytes)
}

// TableCountsAndSizes contains counts and sizes for tables, broken down by
// locality.
type TableCountsAndSizes struct {
	// All contains counts for all tables (local and remote).
	All CountAndSize
	// Local contains counts for local tables only.
	Local CountAndSize
}

// Inc increases the count and size for a single table.
func (cs *TableCountsAndSizes) Inc(tableSize uint64, isLocal bool) {
	cs.All.Inc(tableSize)
	if isLocal {
		cs.Local.Inc(tableSize)
	}
}

// Dec decreases the count and size for a single table.
func (cs *TableCountsAndSizes) Dec(tableSize uint64, isLocal bool) {
	cs.All.Dec(tableSize)
	if isLocal {
		cs.Local.Dec(tableSize)
	}
}

// Accumulate increases the counts and sizes by the given amounts.
func (cs *TableCountsAndSizes) Accumulate(other TableCountsAndSizes) {
	cs.All.Accumulate(other.All)
	cs.Local.Accumulate(other.Local)
}

// Deduct decreases the counts and sizes by the given amounts.
func (cs *TableCountsAndSizes) Deduct(other TableCountsAndSizes) {
	cs.All.Deduct(other.All)
	cs.Local.Deduct(other.Local)
}

// BlobFileCountsAndSizes contains counts and sizes for blob files, broken down
// by locality.
type BlobFileCountsAndSizes struct {
	// All contains counts for all blob files (local and remote).
	All CountAndSize
	// Local contains counts for local blob files only.
	Local CountAndSize
}

// Inc increases the count and size for a single blob file.
func (cs *BlobFileCountsAndSizes) Inc(fileSize uint64, isLocal bool) {
	cs.All.Inc(fileSize)
	if isLocal {
		cs.Local.Inc(fileSize)
	}
}

// Dec decreases the count and size for a single blob file.
func (cs *BlobFileCountsAndSizes) Dec(fileSize uint64, isLocal bool) {
	cs.All.Dec(fileSize)
	if isLocal {
		cs.Local.Dec(fileSize)
	}
}

// Accumulate increases the counts and sizes by the given amounts.
func (cs *BlobFileCountsAndSizes) Accumulate(other BlobFileCountsAndSizes) {
	cs.All.Accumulate(other.All)
	cs.Local.Accumulate(other.Local)
}

// Deduct decreases the counts and sizes by the given amounts.
func (cs *BlobFileCountsAndSizes) Deduct(other BlobFileCountsAndSizes) {
	cs.All.Deduct(other.All)
	cs.Local.Deduct(other.Local)
}

// FileCountsAndSizes contains counts and sizes for all file types.
type FileCountsAndSizes struct {
	// Tables contains counts and sizes for tables.
	Tables TableCountsAndSizes

	// BlobFiles contains counts and sizes for blob files.
	BlobFiles BlobFileCountsAndSizes

	// Other contains counts and sizes for other file types (log, manifest, etc).
	// These are not separated by locality.
	Other CountAndSize
}

func (cs *FileCountsAndSizes) Totals() CountAndSize {
	res := cs.Tables.All
	res.Accumulate(cs.BlobFiles.All)
	res.Accumulate(cs.Other)
	return res
}

// Inc increases the relevant count and size for a single file.
func (cs *FileCountsAndSizes) Inc(fileType FileType, fileSize uint64, isLocal bool) {
	switch fileType {
	case FileTypeTable:
		cs.Tables.Inc(fileSize, isLocal)
	case FileTypeBlob:
		cs.BlobFiles.Inc(fileSize, isLocal)
	default:
		cs.Other.Inc(fileSize)
	}
}

// Dec decreases the relevant count and size for a single file.
func (cs *FileCountsAndSizes) Dec(fileType FileType, fileSize uint64, isLocal bool) {
	switch fileType {
	case FileTypeTable:
		cs.Tables.Dec(fileSize, isLocal)
	case FileTypeBlob:
		cs.BlobFiles.Dec(fileSize, isLocal)
	default:
		cs.Other.Dec(fileSize)
	}
}

// Accumulate increases the counts and sizes by the given amounts.
func (cs *FileCountsAndSizes) Accumulate(other FileCountsAndSizes) {
	cs.Tables.Accumulate(other.Tables)
	cs.BlobFiles.Accumulate(other.BlobFiles)
	cs.Other.Accumulate(other.Other)
}

// Deduct decreases the counts and sizes by the given amounts.
func (cs *FileCountsAndSizes) Deduct(other FileCountsAndSizes) {
	cs.Tables.Deduct(other.Tables)
	cs.BlobFiles.Deduct(other.BlobFiles)
	cs.Other.Deduct(other.Other)
}

type FileType int

// The FileType enumeration.
const (
	FileTypeLog FileType = iota
	FileTypeLock
	FileTypeTable
	FileTypeManifest
	FileTypeOptions
	FileTypeOldTemp
	FileTypeTemp
	FileTypeBlob
)

func SafeSub[T Integer](a, b T) T {
	if a < b {
		panic(fmt.Sprintf("underflow: %d - %d", a, b))
	}
	return a - b
}

type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}
