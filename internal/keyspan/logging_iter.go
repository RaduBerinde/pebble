// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"
	"runtime/debug"

	"github.com/cockroachdb/pebble/internal/base"
)

// Logging wraps an iterator and adds log messages showing each operation and
// its result.
func Logging(iter FragmentIterator, log base.Logger) FragmentIterator {
	debug.Stack()
	return &loggingIter{
		iter:    iter,
		log:     log,
		context: fmt.Sprintf("%T:", iter),
	}
}

// loggingIter is a pass-through FragmentIterator wrapper which performs checks
// on what the wrapped iterator returns.
//
// It verifies that results for various operations are sane, and it optionally
// verifies that spans are within given bounds.
type loggingIter struct {
	iter    FragmentIterator
	log     base.Logger
	context string
}

var _ FragmentIterator = (*loggingIter)(nil)

// SeekGE implements FragmentIterator.
func (i *loggingIter) SeekGE(key []byte) *Span {
	i.log.Infof("%s SeekGE(%q)", i.context, key)
	span := i.iter.SeekGE(key)
	i.log.Infof("%s SeekGE(%q) = %s", i.context, key, span)
	return span
}

// SeekLT implements FragmentIterator.
func (i *loggingIter) SeekLT(key []byte) *Span {
	i.log.Infof("%s SeekLT(%q)", i.context, key)
	span := i.iter.SeekLT(key)
	i.log.Infof("%s SeekLT(%q) = %s", i.context, key, span)
	return span
}

// First implements FragmentIterator.
func (i *loggingIter) First() *Span {
	i.log.Infof("%s First()", i.context)
	span := i.iter.First()
	i.log.Infof("%s First() = %s", i.context, span)
	return span
}

// Last implements FragmentIterator.
func (i *loggingIter) Last() *Span {
	i.log.Infof("%s Last()", i.context)
	span := i.iter.Last()
	i.log.Infof("%s Last() = %s", i.context, span)
	return span
}

// Next implements FragmentIterator.
func (i *loggingIter) Next() *Span {
	i.log.Infof("%s Next()", i.context)
	span := i.iter.Next()
	i.log.Infof("%s Next() = %s", i.context, span)
	return span
}

// Prev implements FragmentIterator.
func (i *loggingIter) Prev() *Span {
	i.log.Infof("%s Prev()", i.context)
	span := i.iter.Prev()
	i.log.Infof("%s Prev() = %s", i.context, span)
	return span
}

// Error implements FragmentIterator.
func (i *loggingIter) Error() error {
	err := i.iter.Error()
	if err != nil {
		i.log.Errorf("%s Error() -> %s", i.context, err)
	}
	return err
}

// Close implements FragmentIterator.
func (i *loggingIter) Close() error {
	i.log.Infof("%s Close()", i.context)
	err := i.iter.Close()
	if err != nil {
		i.log.Errorf("%s Close() -> %s", i.context, err)
	}
	return err
}
