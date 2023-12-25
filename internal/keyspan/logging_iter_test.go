// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
)

func TestLoggingIter(t *testing.T) {
	var spans []Span
	datadriven.RunTest(t, "testdata/logging_iter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			spans = nil
			for _, line := range strings.Split(d.Input, "\n") {
				spans = append(spans, ParseSpan(line))
			}
			return ""

		case "iter":
			l := &base.InMemLogger{}
			iter := Logging(NewIter(base.DefaultComparer.Compare, spans), l)
			runFragmentIteratorCmd(iter, d.Input, nil)
			iter.Close()
			return l.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
