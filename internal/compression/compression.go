// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compression

import (
	"fmt"

	"github.com/minio/minlz"
)

type Algorithm uint8

type AlgorithmWithLevel uint16

// These algorithms have no level and can be used as both an Algorithm and an
// AlgorithmWithLevel.
const (
	None = iota
	Snappy
	nAlgorithmsNoLevel
)

const (
	Zstd Algorithm = nAlgorithmsNoLevel + iota
	MinLZ
	nAlgorithms
)

const (
	ZstdLevel1 = 1<<8 + AlgorithmWithLevel(Zstd)
	ZstdLevel3 = 3<<8 + AlgorithmWithLevel(Zstd)
	ZstdLevel7 = 7<<8 + AlgorithmWithLevel(Zstd)

	MinLZFastest  = minlz.LevelFastest<<8 + AlgorithmWithLevel(MinLZ)
	MinLZBalanced = minlz.LevelBalanced<<8 + AlgorithmWithLevel(MinLZ)
)

// String implements fmt.Stringer, returning a human-readable name for the
// compression algorithm.
func (a Algorithm) String() string {
	switch a {
	case None:
		return "NoCompression"
	case Snappy:
		return "Snappy"
	case Zstd:
		return "ZSTD"
	case MinLZ:
		return "MinLZ"
	default:
		return fmt.Sprintf("unknown(%d)", a)
	}
}

func (al AlgorithmWithLevel) Algorithm() Algorithm {
	return Algorithm(al & 0xFF)
}

func (al AlgorithmWithLevel) level() int {
	return int(al >> 8)
}

func (al AlgorithmWithLevel) String() string {
	if al.level() == 0 {
		return al.Algorithm().String()
	}
	return fmt.Sprintf("%s (level %d)", al.Algorithm(), al.level())
}

type Compressor interface {
	// Compress a block, appending the compressed data to dst[:0].
	Compress(dst, src []byte) []byte

	// Close must be called when the Compressor is no longer needed.
	// After Close is called, the Compressor must not be used again.
	Close()
}

func GetCompressor(a AlgorithmWithLevel) Compressor {
	switch a.Algorithm() {
	case None:
		return noopCompressor{}
	case Snappy:
		return snappyCompressor{}
	case Zstd:
		return getZstdCompressor(a.level())
	case MinLZ:
		return getMinlzCompressor(a.level())
	default:
		panic("Invalid compression type.")
	}
}

type Decompressor interface {
	// DecompressInto decompresses compressed into buf. The buf slice must have the
	// exact size as the decompressed value. Callers may use DecompressedLen to
	// determine the correct size.
	DecompressInto(buf, compressed []byte) error

	// DecompressedLen returns the length of the provided block once decompressed,
	// allowing the caller to allocate a buffer exactly sized to the decompressed
	// payload.
	DecompressedLen(b []byte) (decompressedLen int, err error)

	// Close must be called when the Decompressor is no longer needed.
	// After Close is called, the Decompressor must not be used again.
	Close()
}

func GetDecompressor(a Algorithm) Decompressor {
	switch a {
	case None:
		return noopDecompressor{}
	case Snappy:
		return snappyDecompressor{}
	case Zstd:
		return getZstdDecompressor()
	case MinLZ:
		return minlzDecompressor{}
	default:
		panic("Invalid compression type.")
	}
}
