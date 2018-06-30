package main

import "github.com/apex/log"

type Message struct {
	tag    string
	from   uint64
	query  Query
	to     uint64
	custom interface{}
}

func (msg *Message) log(text string) {
	log.WithFields(log.Fields{
		"tag": msg.tag,
		"from": msg.from,
		"query": msg.query,
		"to": msg.to,
		"custom": msg.custom,
	}).Info(text)
}
