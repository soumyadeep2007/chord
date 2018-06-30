package main

import (
	"sort"
	"math/rand"
	"time"
	"github.com/apex/log"
)

type Ring struct {
	m     int
	nodes []Node
}

func NewRing(m int, nodeIps []string, records []Record) *Ring {
	ring := Ring{m: m, nodes: make([]Node, len(nodeIps))}

	var nodeIds []uint64
	for _, nodeIp := range nodeIps {
		nodeIds = append(nodeIds, calculateId(nodeIp, m))
	}
	sort.Slice(nodeIds, func(i, j int) bool {
		return nodeIds[i] < nodeIds[j]
	})

	sort.Slice(records, func(i, j int) bool {
		return records[i].id < records[j].id
	})

	for i, nodeId := range nodeIds {
		dist := computeDistribution(i, nodeIds, records)
		ring.nodes[i] = *NewNode(nodeId, dist, m)
	}

	for i := range ring.nodes {
		predIndex := i - 1
		if predIndex < 0 {
			predIndex = len(ring.nodes) - 1
		}
		ring.nodes[i].predecessor = &ring.nodes[predIndex]
		ring.nodes[i].successor = &ring.nodes[(i+1)%len(ring.nodes)]
		ring.nodes[i].fingerTable = computeFingerTable(i, ring.nodes, m)
	}

	for i := range ring.nodes {
		ring.nodes[i].start()
	}

	log.WithFields(log.Fields{
		"m":     m,
		"nodes": ring.nodes,
	}).Info("Ring setup with:")

	return &ring
}

func (ring *Ring) Get(key string, client *Client, requestId interface{}) {
	node := ring.connect()
	go ring.performRequest(node, client, key, requestId)
}

func computeDistribution(nodeIndex int, nodeIdsSorted []uint64, recordsSorted []Record) (dist map[uint64]Record) {
	nodeIndexLeft := nodeIndex - 1
	if nodeIndexLeft < 0 {
		nodeIndexLeft = len(nodeIdsSorted) - 1
	}
	// Records with Ids in (lowId, highId] will be distributed to node
	lowId := nodeIdsSorted[nodeIndexLeft]
	highId := nodeIdsSorted[nodeIndex]

	searchKey := lowId
	loRecordIndex := bisectLeftRecord(recordsSorted, searchKey)
	if nodeIdsSorted[nodeIndexLeft] == recordsSorted[loRecordIndex].id {
		loRecordIndex += 1
	}

	dist = make(map[uint64]Record)
	if lowId <= highId {
		i := loRecordIndex
		for recordsSorted[i].id <= highId {
			dist[recordsSorted[i].id] = recordsSorted[i]
			i += 1
		}
	} else {
		for i := loRecordIndex; i < len(recordsSorted); i++ {
			dist[recordsSorted[i].id] = recordsSorted[i]
		}
		i := 0
		for recordsSorted[i].id <= highId {
			dist[recordsSorted[i].id] = recordsSorted[i]
			i += 1
		}
	}

	return dist
}

func computeFingerTable(nodeIndex int, nodesSorted []Node, m int) (fingerTable []*Node) {
	fingerTable = make([]*Node, m)
	id := nodesSorted[nodeIndex].id
	for i := 0; i < m; i++ {
		inc := uint64(1 << uint(i))
		searchKey := (id + inc) % (1 << uint(m))
		index := bisectLeftNode(nodesSorted, searchKey)
		fingerTable[i] = &nodesSorted[index%len(nodesSorted)]
	}

	return fingerTable
}

/*
 * Connects to a node at random
 */
func (ring *Ring) connect() *Node {
	rand.Seed(time.Now().Unix())
	i := rand.Int() % len(ring.nodes)
	return &ring.nodes[i]
}

func (ring *Ring) performRequest(node *Node, client *Client, key string, requestId interface{}) {
	findSuccessorMsg := Message{
		tag:  "find_successor",
		from: client.id,
		query: Query{
			key:       key,
			id:        calculateId(key, ring.m),
			requestId: requestId,
			client:    client,
		},
		to: node.id,
	}

	go ring.receive(client)

	findSuccessorMsg.log("Client sent find_successor message.")
	node.channel <- findSuccessorMsg
}

func (ring *Ring) receive(client *Client) {
	for msg := range client.chResponse {
		switch msg.tag {
		case "successor":
			ring.recvSuccessor(msg)
		case "result":
			ring.recvResult(msg)
		}
	}
}

func (ring *Ring) recvSuccessor(msg Message) {
	msg.log("Client received successor message.")

	successor := msg.custom.(*Node)
	getMsg := Message{
		tag:   "get",
		from:  msg.query.client.id,
		query: msg.query,
		to:    successor.id,
	}

	getMsg.log("Client sent get message.")
	successor.channel <- getMsg
}

func (ring *Ring) recvResult(msg Message) {
	msg.log("Client received result message.")

	result := msg.custom
	log.Infof(">>>>>Query : Key = [%s]; Result: Value = [%v]\n", msg.query.key, result)
}
