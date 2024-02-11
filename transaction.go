package main

import (
	"errors"
	"maps"
)

const maxTransactionDepth = 5

type Transaction struct {
	DataStore
	parentTransaction *Transaction
	nestedTransaction *Transaction
}

func (t *Transaction) startTransaction() (*Transaction, error) {
	if t.nestedTransaction != nil {
		return t.startNestedTransaction(maps.Clone(t.data), 1)
	}

	t.nestedTransaction = &Transaction{
		parentTransaction: t,
		DataStore: DataStore{
			maps.Clone(t.data),
		},
	}

	return t, nil
}

func (t *Transaction) startNestedTransaction(data map[string]string, depth int) (*Transaction, error) {
	if depth > maxTransactionDepth {
		return nil, errors.New("max transaction depth reached")
	}

	if t.nestedTransaction != nil {
		return t.startNestedTransaction(data, depth+1)
	}

	t.nestedTransaction = &Transaction{
		parentTransaction: t,
		DataStore: DataStore{
			data,
		},
	}

	return t, nil
}

func (t *Transaction) getDeepestTransaction() *Transaction {
	if t.nestedTransaction != nil {
		return t.nestedTransaction.getDeepestTransaction()
	}

	return t
}

func (t *Transaction) Get(key string) (*string, error) {
	if t.nestedTransaction != nil {
		return t.getDeepestTransaction().Get(key)
	}

	return t.DataStore.Get(key)
}

func (t *Transaction) Set(key, value string) error {
	if t.nestedTransaction != nil {
		return t.getDeepestTransaction().Set(key, value)
	}

	return t.DataStore.Set(key, value)
}

func (t *Transaction) Delete(key string) error {
	if t.nestedTransaction != nil {
		return t.getDeepestTransaction().Delete(key)
	}

	return t.DataStore.Delete(key)
}

func (t *Transaction) commit() error {
	contextData := t.data
	t.parentTransaction.data = contextData
	t.parentTransaction.nestedTransaction = nil
	t.data = nil
	return nil
}

func (t *Transaction) rollback() error {
	t.parentTransaction.nestedTransaction = nil
	t.data = nil
	return nil
}
