// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build msan

package metamorphic

/*
#include <sanitizer/common_interface_defs.h>
#include <signal.h>

static void msan_go_quit(void) {
  // Keep this tiny: async-signal-safe only.
  raise(SIGQUIT);
}

static void msan_install_sigquit(void) {
  __sanitizer_set_death_callback(msan_go_quit);
}
*/
import "C"

func init() { C.msan_install_sigquit() }
