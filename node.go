package godag

import "time"

type Node struct {
	id         string // id should be unique
	op         Op
	prev       []*Node
	next       []*Node
	timeout    time.Duration
	isCanceled bool
	indegree   int
	costTime   time.Duration
}

func NewStartNode(id string) *Node {
	return &Node{
		id: id,
	}
}

func NewNode(id string, op Op) *Node {
	return &Node{
		id: id,
		op: op,
	}
}

func (n *Node) WithTimeout(timeout time.Duration) *Node {
	n.timeout = timeout
	return n
}

func (n *Node) AddNext(id string, op Op) *Node {
	newNode := Node{
		id:       id,
		op:       op,
		prev:     []*Node{n},
		indegree: 1,
	}
	n.next = append(n.next, &newNode)
	return &newNode
}

func (n *Node) AddNextNode(node *Node) *Node {
	for i := range n.next {
		if n.next[i] == node { // already added
			// panic("already added")
			return node
		}
	}
	
	n.next = append(n.next, node)
	node.prev = append(node.prev, n)
	node.indegree++
	return node
}

func (n *Node) AddPrevNode(node *Node) *Node {
	for i := range n.prev {
		if n.prev[i] == node {
			return node
		}
	}
	node.next = append(node.next, n)
	n.prev = append(n.prev, node)
	n.indegree++
	return node
}

// InsertPrevNode 将node插入prev数组中，位置在after的前面
func (n *Node) InsertPrevNode(node *Node, after *Node) *Node {
	idx := len(n.prev)
	for i := range n.prev {
		if n.prev[i] == node {
			// panic("already added")
			return node
		} else if n.prev[i] == after {
			idx = i
		}
	}

	node.next = append(node.next, n)
	if idx == len(n.prev) {
		n.prev = append(n.prev, node)
	} else {
		n.prev = append(n.prev[:idx+1], n.prev[idx:]...)
		n.prev[idx] = node
	}
	n.indegree++
	return node
}

// FindPrev 从左到右查找Prev是否存在nodes中的任意一个，如果存在则返回最早匹配的node，否则nil
func (n *Node) FindPrev(nodes... *Node) *Node {
	for i := range n.prev {
		for _, node := range nodes {
			if node == n.prev[i] {
				return node
			}
		}
	}
	return nil
}