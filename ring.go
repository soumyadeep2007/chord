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
		ring.nodes = append(ring.nodes, *NewNode(nodeId, dist))
	}

	for i, node := range ring.nodes {
		node.predecessor = ring.nodes[i - 1]
		node.successor = ring.nodes[(i + 1) % len(ring.nodes)]
		node.fingerTable = computeFingerTable(i, ring.nodes, m)
	}

	return &ring
}

func computeDistribution(nodeIndex int, nodeIdsSorted []uint64, recordsSorted []Record) (dist map[uint64]Record) {
	lowId := nodeIdsSorted[nodeIndex-1]
	highId := nodeIdsSorted[nodeIndex]

	searchKey := recordsSorted[lowId].id
	lo := bisectLeftRecord(recordsSorted, searchKey)
	if nodeIdsSorted[nodeIndex-1] == recordsSorted[lo].id {
		lo += 1
	}

	dist = make(map[uint64]Record)
	if lowId <= highId {
		i := lo
		for recordsSorted[i].id <= highId {
			dist[recordsSorted[i].id] = recordsSorted[i]
			i += 1
		}
	} else {
		for i := lo; i < len(recordsSorted); i++ {
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

func computeFingerTable(nodeIndex int, nodesSorted []Node, m uint) (fingerTable []Node) {
	id := nodesSorted[nodeIndex].id
	var i uint
	for i = 0; i < m; i++ {
		inc := uint64(1 << i)
		searchKey := nodesSorted[(id+inc)%(1<<m)].id
		index := bisectLeftNode(nodesSorted, searchKey)
		fingerTable = append(fingerTable, nodesSorted[index])
	}

	return fingerTable
}
