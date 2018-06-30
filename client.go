package main

import "github.com/google/uuid"

type Client struct {
	id         uint64
	ip         string
	chResponse chan Message
	ring       *Ring
}

const NumConcurrentRequests = 1000

func NewClient(ip string, ring *Ring) *Client {
	return &Client{
		ip:         ip,
		id:        	uint64(uuid.ClockSequence()),
		chResponse: make(chan Message, NumConcurrentRequests),
		ring:       ring,
	}
}
