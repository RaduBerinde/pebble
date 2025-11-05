// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import "github.com/cockroachdb/pebble/vfs"

// ObsoleteFile describes a file that is to be deleted.
type ObsoleteFile struct {
	FileType FileType
	FS       vfs.FS
	Path     string
	FileNum  uint64
	FileSize uint64 // approx for log files
	IsLocal  bool
}
