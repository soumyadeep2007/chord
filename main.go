package main

import (
	"bufio"
	"os"
	"log"
	"strings"
)

type message struct {
	tag   string
	value interface{}
}

func readRecords(m uint) (records []Record) {
	file, err := os.Open("records.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		keyValue := strings.Split(scanner.Text(), " ")
		records = append(records, *NewRecord(keyValue[0], keyValue[1], m))
	}

	if !assertIdsUnique(records) {
		os.Exit(2)
	}

	return records
}

func readNodeIps() (nodeIps []string) {
	file, err := os.Open("nodeIps.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		nodeIps = append(nodeIps, scanner.Text())
	}

	return nodeIps
}

func main() {
	var m uint = 9
	records := readRecords(m)
	nodeIps := readNodeIps()
	ring := *NewRing(m, nodeIps, records)
	print(ring.m)
}
