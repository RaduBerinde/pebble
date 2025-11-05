// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build msan

package deletepacer

/*
#include <sanitizer/msan_interface.h>
static inline long msan_test(const void* p, size_t n) {
  return __msan_test_shadow(p, n);   // -1 if clean, else offset of first poisoned byte
}
*/
import "C"
import "unsafe"

func TestString(s string) (firstPoison int) {
	if len(s) == 0 {
		return -1
	}
	p := unsafe.StringData(s)
	return int(C.msan_test(unsafe.Pointer(p), C.size_t(len(s))))
}
