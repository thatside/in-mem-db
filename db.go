package main

import (
	"errors"
	"maps"
)

type InMemDb struct {
	DataStore
	activeTransaction *Transaction
}

func (db *InMemDb) Get(key string) (*string, error) {
	if db.activeTransaction != nil {
		return db.activeTransaction.Get(key)
	}
	return db.DataStore.Get(key)
}

func (db *InMemDb) Set(key, value string) error {
	if db.activeTransaction != nil {
		return db.activeTransaction.Set(key, value)
	}
	return db.DataStore.Set(key, value)
}

func (db *InMemDb) Delete(key string) error {
	if db.activeTransaction != nil {
		return db.activeTransaction.Delete(key)
	}
	return db.DataStore.Delete(key)
}

func (db *InMemDb) StartTransaction() error {
	if db.activeTransaction != nil {
		_, err := db.activeTransaction.startTransaction()
		if err != nil {
			return err
		}
		return nil
	}

	db.activeTransaction = &Transaction{
		DataStore: DataStore{data: maps.Clone(db.data)},
	}

	return nil
}

func (db *InMemDb) Commit() error {
	if db.activeTransaction != nil {
		t := db.activeTransaction.getDeepestTransaction()
		if t.parentTransaction != nil {
			return t.commit()
		}

		db.data = t.data
		t.data = nil
		db.activeTransaction = nil
		return nil
	}

	return errors.New("transaction not started")
}

func (db *InMemDb) Rollback() error {
	if db.activeTransaction != nil {
		t := db.activeTransaction.getDeepestTransaction()
		if t.parentTransaction != nil {
			return t.rollback()
		}

		t.data = nil
		db.activeTransaction = nil
		return nil
	}

	return errors.New("transaction not started")
}
