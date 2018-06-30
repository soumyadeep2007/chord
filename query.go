package main

type Query struct {
	key string
	id uint64
	requestId interface{} //for client-side processing
	client *Client
}
