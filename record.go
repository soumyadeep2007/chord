package main

type Record struct {
	id    uint64
	key   string
	value interface{}
}

func NewRecord(key string, value interface{}, m int) *Record {
	record := Record{id:calculateId(key, m), key:key, value:value}
	return &record
}
