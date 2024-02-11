package main

import (
	"errors"
	"maps"
)

type InMemDb struct {
	DataStore
	activeTransaction *Transaction
}

// this method was introduced to improve readability
func (db *InMemDb) getDataAccessor() DataAccessor {
	if db.activeTransaction != nil {
		return db.activeTransaction
	}

	return &db.DataStore
}

func (db *InMemDb) Get(key string) (*string, error) {
	return db.getDataAccessor().Get(key)
}

func (db *InMemDb) Set(key, value string) error {
	return db.getDataAccessor().Set(key, value)
}

func (db *InMemDb) Delete(key string) error {
	return db.getDataAccessor().Delete(key)
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
		depth:     1,
	}

	return nil
}

func (db *InMemDb) Commit() error {
	if db.activeTransaction == nil {
		return errors.New("transaction not started")
	}

	t := db.activeTransaction.getDeepestTransaction()
	if t.parentTransaction != nil {
		return t.commit()
	}

	db.data = t.data
	t.data = nil
	db.activeTransaction = nil
	return nil
}

func (db *InMemDb) Rollback() error {
	if db.activeTransaction == nil {
		return errors.New("transaction not started")
	}

	t := db.activeTransaction.getDeepestTransaction()
	if t.parentTransaction != nil {
		return t.rollback()
	}

	t.data = nil
	db.activeTransaction = nil
	return nil
}
