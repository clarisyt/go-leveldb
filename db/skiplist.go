package db

import (
	"math/rand"
	"sync"
)

type Key interface{}
type Comparator func(Key, Key) uint8

type Arena struct{}
type Node struct{
	key Key
	mu sync.Mutex
	next_ []*Node
}

type Iterator struct {
	list_ *SkipList
	node_ *Node
}

const (
	kMaxHeight =  12
)

type ISkipList interface {
	Insert(key Key)
	Contains(key Key) bool
}

type SkipList struct {
	list_ *SkipList
	node_ *Node
	mu sync.Mutex
	head_ *Node
	arena_ *Arena
	maxHeight_ uint8
	compare_ Comparator
}

func newNode(key interface{}, height uint8)  (x *Node) {
	x = new(Node)
	x.key = key
	x.next_ = make([]*Node, height)
	return
}

func (node *Node) Next(n uint8) *Node {
	node.mu.Lock()
	defer node.mu.Unlock()

	return node.next_[n]
}

func (node *Node) SetNext(n uint32, x *Node) {
	node.mu.Lock()
	defer node.mu.Unlock()

	node.next_[n] = x
}

func (node *Node) NoBarrierNext(n uint32) *Node {
	return node.next_[n]
}

func (it *Iterator) Valid() bool {
	return it.node_ != nil
}

func (it *Iterator) key() Key {
	return it.node_.key
}

func (it *Iterator) Next() {
	it.node_ = it.node_.Next(0)
}

func (it *Iterator) Prev() {
	it.node_ = it.list_.FindLessThan(it.node_.key)
	if it.node_ == it.list_.head_ {
		it.node_ = nil
	}
}

func (it *Iterator) Seek(target Key) {
	it.node_ = it.list_.FindGreaterOrEqual(target, nil)
}

func (it *Iterator) SeekToLast() {
	it.node_ = it.list_.FindLast()

	if(it.node_ == it.list_.head_) {
		it.node_ = nil
	}
}

func (sl * SkipList) getMaxHeight() uint8 {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	return sl.maxHeight_
}

func (sl *SkipList) randomHeight() uint8 {
	return kMaxHeight;
}

func (sl *SkipList) Equal(a Key, b Key) bool {

}

func (sl *SkipList) RandomHeight() uint8{
	const kBranching uint32 = 4
	var height uint8 = 1
	for height < kMaxHeight &&  (rand.Uint32() % kBranching) == 0  {
		height++
	}
	return height
}

func (sl *SkipList) keyIsAfterNode(key Key, n *Node) bool {
	return n != nil && (sl.compare_(n.key, key) < 0)
}

func (sl *SkipList) FindGreaterOrEqual(key Key, prev []*Node) *Node {
	x := sl.head_
	level := sl.getMaxHeight() - 1
	for {
		next := x.Next(level)
		if sl.keyIsAfterNode(key, next) {
			// Keep searching in this list
			x = next
		} else {
			if prev != nil  {
				prev[level] = x
			}
			if level == 0 {
				return next
			} else {
				// Switch to next list
				level--
			}
		}
	}
}

func (sl *SkipList) FindLessThan(key Key) *Node {
	x := sl.head_
	level := sl.getMaxHeight() - 1
	for {
		next := x.Next(level)
		if next == nil || sl.compare_(x.key, key) >= 0 {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
}

func (sl *SkipList) FindLast() *Node {
	x := sl.head_
	level := sl.getMaxHeight() - 1
	for {
		next := x.Next(level)
		if next == nil {
			if level == 0 {
				return x
			} else {
				// Switch to next list
				level--
			}
		}  else {
			x = next;
		}
	}
}

func (sl *SkipList) Insert(key Key) {
	var prev [kMaxHeight]*Node
	x := sl.FindGreaterOrEqual(key, prev[:])

	height := sl.RandomHeight()
	if height > sl.getMaxHeight() {
		for i := sl.getMaxHeight(); i < height; i++ {
			prev[i] = sl.head_
		}
	}

	x = newNode(key, height)
	for i := 0; i < height; i++ {
		x.NoBarrierNext()
	}

}

func (sl *SkipList) Contains(key Key) bool {
	x := FindGreaterOrEqual(key, nil)
	if x != nil && equal(key, ) {
		return true
	} else {
		return false
	}
}
