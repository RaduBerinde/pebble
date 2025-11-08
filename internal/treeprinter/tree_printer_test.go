// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package treeprinter

import (
	"strings"
	"testing"
)

func TestTreePrinter(t *testing.T) {
	tree := func(n Node) {
		r := n.Child("root")
		r.AddEmptyLine()
		n1 := r.Childf("%d", 1)
		n1.Child("1.1")
		n12 := n1.Child("1.2")
		r.AddEmptyLine()
		r.AddEmptyLine()
		n12.Child("1.2.1")
		r.AddEmptyLine()
		n12.Child("1.2.2")
		n13 := n1.Child("1.3")
		n13.AddEmptyLine()
		n131 := n13.Child("1.3.1")
		n131.AddLine("1.3.1a")
		n13.Child("1.3.2\n1.3.2a")
		n13.AddEmptyLine()
		n131.Child("1.3.1.1\n1.3.1.1a")
		n1.Child("1.4")
		r.Child("2")
	}

	n := New()
	tree(n)

	res := n.String()
	exp := `
root
 │
 ├── 1
 │    ├── 1.1
 │    ├── 1.2
 │    │    │
 │    │    │
 │    │    ├── 1.2.1
 │    │    │
 │    │    └── 1.2.2
 │    ├── 1.3
 │    │    │
 │    │    ├── 1.3.1
 │    │    │   1.3.1a
 │    │    └── 1.3.2
 │    │         │ 1.3.2a
 │    │         │ 1.3.2b
 │    │         │ 1.3.2c
 │    │         └── 1.3.1.1
 │    │             1.3.1.1a
 │    └── 1.4
 └── 2
`
	exp = strings.TrimLeft(exp, "\n")
	if res != exp {
		t.Errorf("incorrect result:\n%s", res)
	}

	n = NewWithStyle(CompactStyle)
	tree(n)
	res = n.String()
	exp = `
root
│
├ 1
│ ├ 1.1
│ ├ 1.2
│ │ │
│ │ │
│ │ ├ 1.2.1
│ │ │
│ │ └ 1.2.2
│ ├ 1.3
│ │ │
│ │ ├ 1.3.1
│ │ │ 1.3.1a
│ │ └ 1.3.2
│ │   1.3.2a
│ │   │
│ │   └ 1.3.1.1
│ │     1.3.1.1a
│ └ 1.4
└ 2
`
	exp = strings.TrimLeft(exp, "\n")
	if res != exp {
		t.Errorf("incorrect result:\n%s", res)
	}

	n = NewWithStyle(BulletStyle)
	tree(n)
	res = n.String()
	exp = `
• root
│
│
├── • 1
│   │
│   ├── • 1.1
│   │
│   ├── • 1.2
│   │   │
│   │   │
│   │   │
│   │   ├── • 1.2.1
│   │   │
│   │   │
│   │   └── • 1.2.2
│   │
│   ├── • 1.3
│   │   │
│   │   │
│   │   ├── • 1.3.1
│   │   │     1.3.1a
│   │   │
│   │   └── • 1.3.2
│   │       │ 1.3.2a
│   │       │
│   │       │
│   │       └── • 1.3.1.1
│   │             1.3.1.1a
│   │
│   └── • 1.4
│
└── • 2
`
	exp = strings.TrimLeft(exp, "\n")
	if res != exp {
		t.Errorf("incorrect result:\n%s", res)
	}
}

func TestTreePrinterUTF(t *testing.T) {
	n := New()

	r := n.Child("root")
	r1 := r.Child("日本語\n本語\n本語")
	r1.Child("日本語\n本語\n本語")
	r.Child("日本語\n本語\n本語")
	res := n.String()
	exp := `
root
 ├── 日本語
 │   本語
 │   本語
 │    └── 日本語
 │        本語
 │        本語
 └── 日本語
     本語
     本語
`

	exp = strings.TrimLeft(exp, "\n")
	if res != exp {
		t.Errorf("incorrect result:\n%s", res)
	}
}

func TestTreePrinterNested(t *testing.T) {
	// The output of a treeprinter can be used as a node inside a larger
	// treeprinter. This is useful when formatting routines use treeprinter
	// internally.
	tp1 := New()
	r1 := tp1.Child("root1")
	r11 := r1.Child("1.1")
	r11.Child("1.1.1")
	r11.Child("1.1.2")
	r1.Child("1.2")
	tree1 := strings.TrimRight(tp1.String(), "\n")

	tp2 := New()
	r2 := tp2.Child("root2")
	r2.Child("2.1")
	r22 := r2.Child("2.2")
	r22.Child("2.2.1")
	tree2 := strings.TrimRight(tp2.String(), "\n")

	tp := New()
	r := tp.Child("tree of trees")
	r.Child(tree1)
	r.Child(tree2)
	res := tp.String()
	exp := `
tree of trees
 ├── root1
 │    ├── 1.1
 │    │    ├── 1.1.1
 │    │    └── 1.1.2
 │    └── 1.2
 └── root2
      ├── 2.1
      └── 2.2
           └── 2.2.1
`

	exp = strings.TrimLeft(exp, "\n")
	if res != exp {
		t.Errorf("incorrect result:\n%s", res)
	}
}

const _ = `
sstable.twoLevelIterator: a@10#1,SET  ← SeekPrefixGE("a", "a", 0) = a@10#1,SET
 ├── colblk.IndexIter: c@7
 └── sstable.singleLevelIterator
      |  a@10#1,SET
      |  file: 000000
      |
      ├── colblk.IndexIter: a@10
      └── colblk.DataBlockIter: a@10#1,SET
`
