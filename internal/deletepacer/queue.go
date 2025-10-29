// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package deletepacer

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/invariants"
)

// Queue implements an allocation efficient FIFO queue. It is not safe for
// concurrent access.
//
// Note that the queue provides pointer access to the internal storage (via
// PeekFront and PushBack) so it must be used with care. These pointers must not
// be used once the respective element is popped out of the queue.
//
// -- Implementation --
//
// The queue is implemented as a linked list of nodes, where each node is a
// small ring buffer. The nodes are allocated using a sync.Pool (a single pool
// should be created for any given type and is used for all queues of that
// type).
type Queue struct {
	len        int
	head, tail *queueNode
}

// MakeQueue constructs a new Queue.
func MakeQueue() Queue {
	return Queue{}
}

// Len returns the current length of the queue.
func (q *Queue) Len() int {
	return q.len
}

// PushBack adds t to the end of the queue.
//
// The returned pointer can be used to modify the element while it is in the
// queue; it is valid until the element is removed from the queue.
func (q *Queue) PushBack(t queueEntry) *queueEntry {
	if q.head == nil {
		//q.head = q.pool.get()
		q.head = &queueNode{}
		q.tail = q.head
		fmt.Printf("allocated queueNode %p\n", q.head)
	} else if q.tail.IsFull() {
		//newTail := q.pool.get()
		newTail := &queueNode{}
		q.tail.next = newTail
		q.tail = newTail
	}
	q.len++
	return q.tail.PushBack(t)
}

// PeekFront returns the current head of the queue, or nil if the queue is
// empty.
//
// The result is only valid until the next call to PopFront.
func (q *Queue) PeekFront() *queueEntry {
	if q.len == 0 {
		return nil
	}
	return q.head.PeekFront()
}

//go:noinline
func (q *Queue) Front() queueEntry {
	return q.head.Front()
}

// PopFront removes the current head of the queue.
//
// It is illegal to call PopFront on an empty queue.
//
//go:noinline
func (q *Queue) PopFront() {
	q.head.PopFront()
	// If this is the only node, we don't want to release it; otherwise we would
	// allocate/free a node every time we transition between the queue being empty
	// and non-empty.
	if q.head.len == 0 && q.head.next != nil {
		oldHead := q.head
		q.head = oldHead.next
		//q.pool.put(oldHead)
	}
	q.len--
}

// We batch the allocation of this many queue objects. The value was chosen
// without experimentation - it provides a reasonable amount of amortization
// without a very large increase in memory overhead if T is large.
const queueNodeSize = 8

type queueNode struct {
	buf       [queueNodeSize]queueEntry
	head, len int32
	next      *queueNode
}

func (qn *queueNode) IsFull() bool {
	return qn.len == queueNodeSize
}

func (qn *queueNode) PushBack(t queueEntry) *queueEntry {
	if invariants.Enabled && qn.len >= queueNodeSize {
		panic("cannot push back into a full node")
	}
	i := (qn.head + qn.len) % queueNodeSize
	qn.buf[i] = t
	qn.len++
	return &qn.buf[i]
}

func (qn *queueNode) PeekFront() *queueEntry {
	if qn.len == 0 {
		panic("empty\n")
	}
	return &qn.buf[qn.head]
}

func (qn *queueNode) Front() queueEntry {
	if qn.len == 0 {
		panic("empty\n")
	}
	return qn.buf[qn.head]
}

func (qn *queueNode) PopFront() queueEntry {
	if invariants.Enabled && qn.len == 0 {
		panic("cannot pop from empty queue")
	}
	t := qn.buf[qn.head]
	qn.buf[qn.head] = queueEntry{}
	qn.head = (qn.head + 1) % queueNodeSize
	qn.len--
	return t
}
