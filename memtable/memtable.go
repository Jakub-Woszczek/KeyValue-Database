package memtable

import (
	"bytes"
	// "fmt"

	// "testing/quick"

	datastructures "github.com/Jakub-Woszczek/kvdb/dataStructures"
)

type Memtable struct {
	Root       *Node
	Size       int   // Total number of nodes
	KeysSize   int64 // Total bytes of all keys
	ValuesSize int64 // Total bytes of all values
}

type Node struct {
	Key         []byte
	Value       []byte
	Left        *Node
	Right       *Node
	Parent      *Node
	IsTombstone bool
	Color       bool // True for black, false for red
}

func NewMemtable() *Memtable {
	return &Memtable{
		Root: nil,
		Size: 0,
	}
}

func (mTable *Memtable) Get(key []byte) (value []byte, found bool) {
	x := mTable.Root
	if x == nil {
		return nil, false
	}

	for x != nil {
		switch bytes.Compare(key, x.Key) {
		case 0:
			return x.Value, true
		case -1:
			x = x.Left
		case 1:
			x = x.Right
		}
	}
	return nil, false
}

func (mTable *Memtable) Insert(key []byte, value []byte) {
	newNode, isReplaced, oldValLen := mTable.RBInsert(key, value)
	if !isReplaced {
		mTable.Size++
		mTable.KeysSize += int64(len(key))
		mTable.ValuesSize += int64(len(value))
	} else {
		mTable.ValuesSize += int64(len(value)) - oldValLen
	}
	if newNode == nil {
		// new node is root or value was updated
		return
	}

	// Fix the red-black tree properties after insertion
	mTable.fixInsert(newNode)
}

// Cormen et al. Introduction to Algorithms, 4rd Edition, Chapter 13.3
func (mTable *Memtable) fixInsert(z *Node) {
	for z.Parent != nil && !z.Parent.Color {
		if z.Parent == z.Parent.Parent.Left {
			y := z.Parent.Parent.Right // uncle
			if y != nil && !y.Color {
				z.Parent.Color = true
				y.Color = true
				z.Parent.Parent.Color = false
				z = z.Parent.Parent
			} else {
				if z == z.Parent.Right {
					z = z.Parent
					mTable.RotateLeft(z)
				}
				z.Parent.Color = true
				z.Parent.Parent.Color = false
				mTable.RotateRight(z.Parent.Parent)
			}
		} else { // parent is a right child (mirror)
			y := z.Parent.Parent.Left // uncle
			if y != nil && !y.Color {
				z.Parent.Color = true
				y.Color = true
				z.Parent.Parent.Color = false
				z = z.Parent.Parent
			} else {
				if z == z.Parent.Left {
					z = z.Parent
					mTable.RotateRight(z)
				}
				z.Parent.Color = true
				z.Parent.Parent.Color = false
				mTable.RotateLeft(z.Parent.Parent)
			}
		}
	}
	mTable.Root.Color = true
}

func (mTable *Memtable) RBInsert(key []byte, value []byte) (node *Node, isReplaced bool, oldValLen int64) {
	if mTable.Root == nil {
		mTable.Root = &Node{Key: key, Value: value, Color: true} // root is always black
		return nil, false, 0
	}

	// Search for the correct position to insert the new x
	x := mTable.Root
	for x != nil {
		switch bytes.Compare(key, x.Key) {
		// case 0: update value
		case 0:
			oldValLen = int64(len(x.Value)) // Should not cause problems (if on save validate valLen < 2**32-1)
			x.Value = value
			return nil, true, oldValLen
		// case -1 key in smaller than node key
		case -1:
			if x.Left == nil {
				y := &Node{Key: key, Value: value, Color: false}
				x.Left = y
				y.Parent = x
				return y, false, 0
			}
			x = x.Left
		// case 1: key is greater than node key
		case 1:
			if x.Right == nil {
				y := &Node{Key: key, Value: value, Color: false}
				x.Right = y
				y.Parent = x
				return y, false, 0
			}
			x = x.Right
		}
	}
	return nil, false, 0
}

// Cormen et al. Introduction to Algorithms, 4rd Edition, Chapter 13.2
func (T *Memtable) RotateLeft(x *Node) {
	if x.Right == nil {
		return
	}
	y := x.Right
	x.Right = y.Left
	if y.Left != nil {
		y.Left.Parent = x
	}
	y.Parent = x.Parent
	if x.Parent == nil {
		T.Root = y
	} else if x == x.Parent.Left {
		x.Parent.Left = y
	} else {
		x.Parent.Right = y
	}
	y.Left = x
	x.Parent = y
}

// Cormen et al. Introduction to Algorithms, 4rd Edition, Chapter 13.2
func (T *Memtable) RotateRight(x *Node) {
	if x.Left == nil {
		return
	}
	y := x.Left
	x.Left = y.Right
	if y.Right != nil {
		y.Right.Parent = x
	}
	y.Parent = x.Parent
	if x.Parent == nil {
		T.Root = y
	} else if x == x.Parent.Right {
		x.Parent.Right = y
	} else {
		x.Parent.Left = y
	}
	y.Right = x
	x.Parent = y
}

type MemtableIterator struct {
	stack *datastructures.Stack[*Node]
}

func NewMemtableIterator(root *Node) *MemtableIterator {
	iter := &MemtableIterator{
		stack: &datastructures.Stack[*Node]{},
	}
	iter.pushLeft(root)
	return iter
}

func (iter *MemtableIterator) pushLeft(node *Node) {
	for node != nil {
		iter.stack.Push(node)
		node = node.Left
	}
}

func (iter *MemtableIterator) HasNext() bool {
	return len(*iter.stack) > 0
}

func (iter *MemtableIterator) Next() (*Node, bool) {
	if !iter.HasNext() {
		return nil, false
	}

	node, valid := iter.stack.Pop()
	if !valid {
		return node, valid
	}

	iter.pushLeft(node.Right)
	return node, valid
}
