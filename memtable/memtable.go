package memtable

import "bytes"

type MEMTABLE struct {
	Root *Node
}

type Node struct {
	Key         []byte
	value       []byte
	left        *Node
	right       *Node
	parent      *Node
	isTombstone bool
	color       bool // True for black, false for red
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
	for z.parent != nil && !z.parent.color {
		if z.parent == z.parent.parent.left {
			y := z.parent.parent.right // uncle
			if y != nil && !y.color {
				z.parent.color = true
				y.color = true
				z.parent.parent.color = false
				z = z.parent.parent
			} else {
				if z == z.parent.right {
					z = z.parent
					mTable.RotateLeft(z)
				}
				z.parent.color = true
				z.parent.parent.color = false
				mTable.RotateRight(z.parent.parent)
			}
		} else { // parent is a right child (mirror)
			y := z.parent.parent.left // uncle
			if y != nil && !y.color {
				z.parent.color = true
				y.color = true
				z.parent.parent.color = false
				z = z.parent.parent
			} else {
				if z == z.parent.left {
					z = z.parent
					mTable.RotateRight(z)
				}
				z.parent.color = true
				z.parent.parent.color = false
				mTable.RotateLeft(z.parent.parent)
			}
		}
	}
	mTable.Root.color = true
}

func (mTable *MEMTABLE) RBInsert(key []byte, value []byte) *Node {
	if mTable.Root == nil {
		mTable.Root = &Node{Key: key, value: value, color: true} // root is always black
		return nil
	}

	// Search for the correct position to insert the new x
	x := mTable.Root
	for x != nil {
		switch bytes.Compare(key, x.Key) {
		// case -1 or 0: key in smaller than node key
		case -1, 0:
			if x.left == nil {
				y := &Node{Key: key, value: value, color: false}
				x.left = y
				y.parent = x
				return y
			}
			x = x.left
		// case 1: key is greater than node key
		case 1:
			if x.right == nil {
				y := &Node{Key: key, value: value, color: false}
				x.right = y
				y.parent = x
				return y
			}
			x = x.right
		}
	}
	return nil
}

// Cormen et al. Introduction to Algorithms, 4rd Edition, Chapter 13.2
func (T *MEMTABLE) RotateLeft(x *Node) {
	if x.right == nil {
		return
	}
	y := x.right
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		T.Root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
	return
}

// Cormen et al. Introduction to Algorithms, 4rd Edition, Chapter 13.2
func (T *MEMTABLE) RotateRight(x *Node) {
	if x.left == nil {
		return
	}
	y := x.left
	x.left = y.right
	if y.right != nil {
		y.right.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		T.Root = y
	} else if x == x.parent.right {
		x.parent.right = y
	} else {
		x.parent.left = y
	}
	y.right = x
	x.parent = y
	return
}
