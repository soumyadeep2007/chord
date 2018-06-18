package main

import (
	"sort"
	"crypto/sha1"
	"encoding/binary"
)

func bisectLeftRecord(items []Record, searchKey uint64) int {
	return sort.Search(len(items), func(i int) bool { return items[i].id >= searchKey })
}

func bisectLeftNode(items []Node, searchKey uint64) int {
	return sort.Search(len(items), func(i int) bool { return items[i].id >= searchKey })
}

func calculateId(key string, m uint) (uint64) {
	h := sha1.New()
	h.Write([]byte(key))
	hashedKey := binary.BigEndian.Uint64(h.Sum(nil))
	return hashedKey % (1 << m)
}

func assertIdsUnique(records []Record) bool {
	idSet := make(map[uint64]bool)
	for _, record := range records {
		idSet[record.id] = true
	}

	if len(idSet) != len(records) {
		return false
	}

	return true
}
