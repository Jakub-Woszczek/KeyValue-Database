package memtable

import "bytes"

type MEMTABLE struct {
	Root *Node
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

func NewMemtable() *MEMTABLE {
	return &MEMTABLE{Root: nil}
}

func (mTable *MEMTABLE) Insert(key []byte, value []byte) {
	newNode := mTable.RBInsert(key, value)
	if newNode == nil {
		// new node is root
		return
	}

	// Fix the red-black tree properties after insertion
	mTable.fixInsert(newNode)
}

// Cormen et al. Introduction to Algorithms, 4rd Edition, Chapter 13.3
func (mTable *MEMTABLE) fixInsert(z *Node) {
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

func (mTable *MEMTABLE) RBInsert(key []byte, value []byte) *Node {
	if mTable.Root == nil {
		mTable.Root = &Node{Key: key, Value: value, Color: true} // root is always black
		return nil
	}

	// Search for the correct position to insert the new x
	x := mTable.Root
	for x != nil {
		switch bytes.Compare(key, x.Key) {
		// case -1 or 0: key in smaller than node key
		case -1, 0:
			if x.Left == nil {
				y := &Node{Key: key, Value: value, Color: false}
				x.Left = y
				y.Parent = x
				return y
			}
			x = x.Left
		// case 1: key is greater than node key
		case 1:
			if x.Right == nil {
				y := &Node{Key: key, Value: value, Color: false}
				x.Right = y
				y.Parent = x
				return y
			}
			x = x.Right
		}
	}
	return nil
}

// Cormen et al. Introduction to Algorithms, 4rd Edition, Chapter 13.2
func (T *MEMTABLE) RotateLeft(x *Node) {
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
func (T *MEMTABLE) RotateRight(x *Node) {
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
