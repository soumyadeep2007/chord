package main

import (
	"github.com/apex/log"
	"fmt"
	"math"
)

type Node struct {
	id          uint64
	m           int
	dist        map[uint64]Record
	channel     chan Message
	predecessor *Node
	successor   *Node
	fingerTable []*Node
}

func NewNode(id uint64, dist map[uint64]Record, m int) *Node {
	node := Node{
		id:       id,
		m:        m,
		dist:     dist,
		channel:  make(chan Message, 1000),
	}

	return &node
}

func (node *Node) start() {
	log.WithFields(log.Fields{
		"id": node.id,
		"m": node.m,
		"fingerTable": node.dumpFingerTable(),
	}).Info("Node started:")

	go node.receive()
}

func (node *Node) receive() {
	for msg := range node.channel {
		switch msg.tag {
		case "find_successor":
			node.recvFindSuccessor(msg)
		case "get":
			node.recvGet(msg)
		}
	}
}

func (node *Node) recvFindSuccessor(msg Message) {
	msg.log("Node received find_successor message.")

	s := node.id
	e := node.successor.id
	if node.isKeyInRange(s, e, msg.query.id, false, true) {
		successorMsg := Message{
			tag:   "successor",
			from:  node.id,
			query: msg.query,
			to:    msg.query.client.id,
			custom: node.successor,
		}

		successorMsg.log("Node sent successor message.")

		msg.query.client.chResponse <- successorMsg
	} else {
		cpNode := node.findClosestPrecedingNode(msg.query.id)
		findSuccessorMsg := Message{
			tag:   "find_successor",
			from:  node.id,
			query: msg.query,
			to:    cpNode.id,
		}

		findSuccessorMsg.log("Node sent find_successor message.")

		cpNode.channel <- findSuccessorMsg
	}
}

func (node *Node) recvGet(msg Message) {
	msg.log("Node received get message.")

	resultMsg := Message{
		tag: "result",
		from: node.id,
		query: msg.query,
		to: msg.query.client.id,
		custom: node.dist[msg.query.id],
	}

	resultMsg.log("Node sent result.")

	msg.query.client.chResponse <- resultMsg
}

/*
 * Is id in (s, e)?
 * Note : We also have to keep in mind that it is a ring!
 */
func (node *Node) isKeyInRange(s, e, id uint64, sinc bool, einc bool) bool {
	if id < s {
		id += (1 << uint(node.m)) - 1
	}
	if e < s {
		e += (1 << uint(node.m)) - 1
	}

	var sCond, eCond bool
	if sinc {
		sCond = id >= s
	} else {
		sCond = id > s
	}
	if einc {
		eCond = id <= e
	} else {
		eCond = id < e
	}

	return sCond && eCond
}

func (node *Node) findClosestPrecedingNode(id uint64) (cpNode *Node) {
	for i := node.m - 1; i >= 0; i-- {
		if node.isKeyInRange(node.id, id, node.fingerTable[i].id, false, false) {
			cpNode = node.fingerTable[i]
			break
		}
	}

	if cpNode == nil {
		cpNode = node.fingerTable[len(node.fingerTable) - 1]
	}

	return cpNode
}

func (node *Node) dumpFingerTable() string {
	dump := ""
	for i, entry := range node.fingerTable {
		dump += fmt.Sprintf("(%d, %d); ",
			int(float64(node.id) + math.Pow(2, float64(i))), entry.id)
	}
	return dump
}
