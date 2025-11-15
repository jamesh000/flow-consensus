package main

import (
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v2"
)

func TempDir() string {
	dir, err := os.MkdirTemp("", "flow-testing-temp-")
	if err != nil {
		panic(err)
	}
	return dir
}

func badgerDB(dir string, create func(badger.Options) (*badger.DB, error)) *badger.DB {
	opts := badger.
		DefaultOptions(dir).
		WithKeepL0InMemory(true).
		WithLogger(nil)
	db, err := create(opts)
	if err != nil {
		panic(err)
	}
	return db
}

func BadgerDB(dir string) *badger.DB {
	return badgerDB(dir, badger.Open)
}

func TempBadgerDB() (*badger.DB, string) {
	dir := TempDir()
	db := BadgerDB(dir)
	return db, dir
}

func TempPebbleDB() (*pebble.DB, string) {
	dir := TempDir()
	db, err := pebble.Open(dir, &pebble.Options{
		FormatMajorVersion: pebble.FormatNewest,
	})
	if err != nil {
		panic(err)
	}
	return db, dir
}
