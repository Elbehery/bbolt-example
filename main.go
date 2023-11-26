package main

import (
	"go.etcd.io/bbolt"
	"log"
	"time"
)

func main() {
	db, err := bbolt.Open("my.db", 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}
