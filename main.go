package main

import (
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"log"
	"time"
)

const (
	databasePath = "sample.db"
	sampleBucket = "sampleBucket"
	key1         = "Ben"
	value1       = "Awesome"
	key2         = "Etcd"
	value2       = "is the heart of K8s"
)

var (
	ErrCreatingSampleBucket = errors.New("creating bucket failed")
)

func main() {

	// open a database file
	db, err := bolt.Open(databasePath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// create a test bucket
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(sampleBucket))
		if err != nil {
			return fmt.Errorf("failed creating bucket '%v': %v", sampleBucket, err)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// insert data into the bucket
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sampleBucket))
		if b == nil {
			return fmt.Errorf("failed retrieving bucket '%v': '%v'", sampleBucket, bolt.ErrBucketNotFound)
		}

		err = b.Put([]byte(key1), []byte(value1))
		if err != nil {
			return fmt.Errorf("failed inserting data in bucket '%v': '%v'", sampleBucket, err)
		}

		err = b.Put([]byte(key2), []byte(value2))
		if err != nil {
			return fmt.Errorf("failed inserting data in bucket '%v': '%v'", sampleBucket, err)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// closing the db
	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}

	// re-opening
	db, err = bolt.Open(databasePath, 0600, &bolt.Options{Timeout: 1 * time.Second})

	// retrieving the data
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sampleBucket))
		if b == nil {
			return fmt.Errorf("failed retrieving bucket '%v': '%v'", sampleBucket, bolt.ErrBucketNotFound)
		}

		v := b.Get([]byte(key1))
		if string(v) != value1 {
			return fmt.Errorf("incorrect data with key '%v', expected value '%v', but got '%v' instead", key1, value1, string(v))
		}

		v = b.Get([]byte(key2))
		if string(v) != value2 {
			return fmt.Errorf("incorrect data with key '%v', expected value '%v', but got '%v' instead", key2, value2, string(v))
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// deleting key1 from bucket
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sampleBucket))
		if b == nil {
			return fmt.Errorf("failed retrieving bucket '%v': '%v'", sampleBucket, bolt.ErrBucketNotFound)
		}
		err := b.Delete([]byte(key1))
		if err != nil {
			return fmt.Errorf("failed deleting key '%v' from bucket '%v'", key1, sampleBucket)
		}

		// try retrieve delete key
		v := b.Get([]byte(key1))
		if v != nil {
			return fmt.Errorf("key '%v' still exist after deletion", key1)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// deleting the bucket
	db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(sampleBucket))
		if err != nil {
			return fmt.Errorf("failed deleting bucket '%v': '%v'", sampleBucket, err)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// close the db again
	db.Close()
	if err != nil {
		log.Fatal(err)
	}

	db, err = bolt.Open(databasePath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sampleBucket))
		if b != nil {
			return fmt.Errorf("found bucket %v after deletion", sampleBucket)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
