package main

import (
	"bufio"
	"os"
	"strings"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	var m = 9
	records := readRecords(m)
	nodeIps := readNodeIps()
	ring := *NewRing(m, nodeIps, records)
	client := NewClient("127.0.0.1", &ring)
	executeQueries(client, &ring)

	select {}
}

func readRecords(m int) (records []Record) {
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
		log.Fatalf("Non unique keys")
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

func executeQueries(client *Client, ring *Ring) {
	file, err := os.Open("workload.txt")
	if err != nil {
		log.Fatal("Couldn't open records file..exiting..")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Connect to a random client every time and query
		key := scanner.Text()
		ring.Get(key, client, generateNextRequestId())
	}
}

var currRequestId = 0

func generateNextRequestId() int {
	currRequestId += 1
	return currRequestId
}
