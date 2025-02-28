// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// NB: this list of tags is taken from encoding/binary/native_endian_big.go
//go:build armbe || arm64be || m68k || mips || mips64 || mips64p32 || ppc || ppc64 || s390 || s390x || shbe || sparc || sparc64

package buildtags

// BigEndian is true if the target platform is big endian.
const BigEndian = true
