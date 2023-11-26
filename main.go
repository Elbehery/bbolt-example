package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"go.etcd.io/bbolt"
)

const (
	dbFile     = "my.db"
	bucketName = "tstBucket"
	tstKey     = "tstKey"
	tstValue   = "tstValue"
)

func main() {
	db, err := bbolt.Open(dbFile, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// create a bucket
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("error creating bucket %v: %v", bucketName, err)
		}
		return nil
	})

	// put key/value pair in the bucket
	err = db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("could not find bucket %v: %v", bucketName, err)
		}
		// insert
		pErr := b.Put([]byte(tstKey), []byte(tstValue))
		if pErr != nil {
			return fmt.Errorf("can not insert key %v, and value %v into bucket %v: %v", tstKey, tstValue, bucketName, pErr)
		}
		return nil
	})

	// retrieve value from bucket by key
	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("could not find bucket %v: %v", bucketName, err)
		}
		v := b.Get([]byte(tstKey))
		fmt.Printf("key %v has value %s\n", tstKey, v)
		return nil
	})

	// insert random keys and values
	err = db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("could not find bucket %v: %v", bucketName, err)
		}

		for i := 0; i < 10; i++ {
			pErr := b.Put(itob(i), itob(i*1000))
			if pErr != nil {
				return fmt.Errorf("can not insert key %v, and value %v into bucket %v: %v", i, i*1000, bucketName, pErr)
			}
		}
		return nil
	})

	fmt.Println("cursor ....")
	// retrieving using cursor
	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("could not find bucket %v: %v", bucketName, err)
		}
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}
		return nil
	})

	fmt.Println("forEach()...")
	// using ForEach()
	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("could not find bucket %v: %v", bucketName, err)
		}

		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			return nil
		})

		return nil
	})
}

func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
