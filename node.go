package main

type Node struct {
	id          uint64
	dist        map[uint64]Record
	channel     chan message
	predecessor Node
	successor   Node
	fingerTable []Node
}

func NewNode(id uint64, dist map[uint64]Record) *Node {
	node := Node{id: id, dist: dist, channel: make(chan message)}

	return &node
}
