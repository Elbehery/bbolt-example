package main

import (
	bolt "go.etcd.io/bbolt"
	"log"
)

func main() {
	path := "./db-dir"
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
