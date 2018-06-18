package main

import (
	"sort"
)

type Ring struct {
	m     uint
	nodes []Node
}

func NewRing(m uint, nodeIps []string, records []Record) *Ring {
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
		ring.nodes[i] = *NewNode(nodeId, dist)
	}

	for i := range ring.nodes {
		predIndex := i - 1
		if predIndex < 0 {
			predIndex = len(ring.nodes) - 1
		}
		ring.nodes[i].predecessor = &ring.nodes[predIndex]
		ring.nodes[i].successor = &ring.nodes[(i + 1) % len(ring.nodes)]
		ring.nodes[i].fingerTable = computeFingerTable(i, ring.nodes, m)
	}

	return &ring
}

func computeDistribution(nodeIndex int, nodeIdsSorted []uint64, recordsSorted []Record) (dist map[uint64]Record) {
	nodeIndexLeft := nodeIndex-1
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

func computeFingerTable(nodeIndex int, nodesSorted []Node, m uint) (fingerTable []*Node) {
	fingerTable = make([]*Node, m)
	id := nodesSorted[nodeIndex].id
	var i uint
	for i = 0; i < m; i++ {
		inc := uint64(1 << i)
		searchKey := (id+inc)%(1<<m)
		index := bisectLeftNode(nodesSorted, searchKey)
		fingerTable[i] = &nodesSorted[index % len(nodesSorted)]
	}

	return fingerTable
}
