package main

import (
	"errors"
	"maps"
)

// the limit is arbitrary here as Go can support much deeper recursive calls
const maxTransactionDepth = 5

type Transaction struct {
	DataStore
	depth             int
	parentTransaction *Transaction
	nestedTransaction *Transaction
}

func (t *Transaction) startTransaction() (*Transaction, error) {
	deepestTransaction := t.getDeepestTransaction()

	resultDepth := deepestTransaction.depth + 1
	if resultDepth > maxTransactionDepth {
		return nil, errors.New("max transaction depth reached")
	}

	// we want a full copy of parent transaction data here to pass to the child
	data := maps.Clone(deepestTransaction.data)

	deepestTransaction.nestedTransaction = &Transaction{
		parentTransaction: deepestTransaction,
		depth:             deepestTransaction.depth + 1,
		DataStore: DataStore{
			data,
		},
	}

	return deepestTransaction, nil
}

func (t *Transaction) getDeepestTransaction() *Transaction {
	if t.nestedTransaction != nil {
		return t.nestedTransaction.getDeepestTransaction()
	}

	return t
}

func (t *Transaction) Get(key string) (*string, error) {
	return t.getDeepestTransaction().DataStore.Get(key)
}

func (t *Transaction) Set(key, value string) error {
	return t.getDeepestTransaction().DataStore.Set(key, value)
}

func (t *Transaction) Delete(key string) error {
	return t.getDeepestTransaction().DataStore.Delete(key)
}

func (t *Transaction) commit() error {
	if t.parentTransaction == nil {
		return errors.New("no parent transaction to commit to")
	}
	// update parent transaction data
	t.parentTransaction.data = t.data
	// clean up all references and data
	t.parentTransaction.nestedTransaction = nil
	t.data = nil
	t.parentTransaction = nil
	return nil
}

func (t *Transaction) rollback() error {
	if t.parentTransaction == nil {
		return errors.New("no parent transaction to rollback onto")
	}
	// clean up all references and data
	t.parentTransaction.nestedTransaction = nil
	t.data = nil
	t.parentTransaction = nil
	return nil
}
