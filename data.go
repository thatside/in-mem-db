package main

import "errors"

type DataAccessor interface {
	Get(key string) (*string, error)
	Set(key, value string) error
	Delete(key string) error
}

// the structure of stored data is the same everywhere so I have created an embeddable struct for that
type DataStore struct {
	data map[string]string
}

func (ds *DataStore) Get(key string) (*string, error) {
	value, ok := ds.data[key]
	if !ok {
		return nil, errors.New("key not found")
	}

	return &value, nil
}

func (ds *DataStore) Set(key, value string) error {
	ds.data[key] = value
	return nil
}

func (ds *DataStore) Delete(key string) error {
	delete(ds.data, key)
	return nil
}
