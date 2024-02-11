package main

import "testing"

const (
	key1   = "key1"
	key2   = "key2"
	value1 = "value1"
	value2 = "value2"
)

func TestSimpleTransactionCommit(t *testing.T) {
	db := InMemDb{DataStore: DataStore{map[string]string{}}}
	db.Set(key1, value1)
	db.StartTransaction()
	db.Set(key1, value2)
	key1Value, _ := db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a uncommited transaction value %s, got %s", value2, *key1Value)
	}
	db.Commit()
	key1Value, _ = db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a commited transaction value %s, got %s", value2, *key1Value)
	}
}

func TestSimpleTransactionRollback(t *testing.T) {
	db := InMemDb{DataStore: DataStore{map[string]string{}}}
	db.Set(key1, value1)
	db.StartTransaction()
	db.Set(key1, value2)
	key1Value, _ := db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a uncommited transaction value %s, got %s", value2, *key1Value)
	}
	db.Rollback()
	key1Value, _ = db.Get(key1)
	if *key1Value != value1 {
		t.Fatalf("Expected to get value before transaction %s, got %s", value1, *key1Value)
	}
}

func TestNestedTransactionCommitWithSet(t *testing.T) {
	db := InMemDb{DataStore{data: map[string]string{}}, nil}
	db.Set(key1, value1)
	db.StartTransaction()
	db.Set(key1, value2)
	key1Value, _ := db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a uncommited transaction value %s, got %s", value2, *key1Value)
	}
	db.StartTransaction()
	db.Set(key1, "value3")
	key1Value, _ = db.Get(key1)
	if *key1Value != "value3" {
		t.Fatalf("Expected to get a uncommited nested transaction value %s, got %s", "value3", *key1Value)
	}
	db.StartTransaction()
	db.Set(key1, "value4")
	key1Value, _ = db.Get(key1)
	if *key1Value != "value4" {
		t.Fatalf("Expected to get a uncommited nested nested transaction value %s, got %s", "value4", *key1Value)
	}
	db.Commit()
	key1Value, _ = db.Get(key1)
	if *key1Value != "value4" {
		t.Fatalf("Expected to get a uncommited nested nested transaction value %s, got %s", "value4", *key1Value)
	}
	db.Commit()
	key1Value, _ = db.Get(key1)
	if *key1Value != "value4" {
		t.Fatalf("Expected to get a uncommited nested nested transaction value %s, got %s", "value4", *key1Value)
	}
	db.Commit()
	key1Value, _ = db.Get(key1)
	if *key1Value != "value4" {
		t.Fatalf("Expected to get a uncommited nested nested transaction value %s, got %s", "value4", *key1Value)
	}
}

func TestNestedTransactionCommit(t *testing.T) {
	db := InMemDb{DataStore{data: map[string]string{}}, nil}
	db.Set(key1, value1)
	db.StartTransaction()
	db.Set(key1, value2)
	key1Value, _ := db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a uncommited transaction value %s, got %s", value2, *key1Value)
	}
	db.StartTransaction()
	key1Value, _ = db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a uncommited nested transaction value %s, got %s", value2, *key1Value)
	}
	db.Delete(key1)
	key1Value, _ = db.Get(key1)
	if key1Value != nil {
		t.Fatal("Expected to get a nil result for deleted uncommited nested transaction value")
	}
	db.Commit()
	key1Value, _ = db.Get(key1)
	if key1Value != nil {
		t.Fatal("Expected to get a nil result for deleted commited transaction value")
	}
	db.Commit()
	key1Value, _ = db.Get(key1)
	if key1Value != nil {
		t.Fatal("Expected to get a nil result for deleted commited transaction value")
	}
}

func TestNestedTransactionRollback(t *testing.T) {
	db := InMemDb{DataStore{data: map[string]string{}}, nil}
	db.Set(key1, value1)
	db.StartTransaction()
	db.Set(key1, value2)
	key1Value, _ := db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a uncommited transaction value %s, got %s", value2, *key1Value)
	}
	db.StartTransaction()
	key1Value, _ = db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a uncommited nested transaction value %s, got %s", value2, *key1Value)
	}
	db.Delete(key1)
	key1Value, _ = db.Get(key1)
	if key1Value != nil {
		t.Fatal("Expected to get a nil result for deleted uncommitted nested transaction value")
	}
	db.Rollback()
	key1Value, _ = db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a uncommited rolled back transaction value %s, got %s", value2, *key1Value)
	}
	db.Commit()

	key1Value, _ = db.Get(key1)
	if *key1Value != value2 {
		t.Fatalf("Expected to get a committed rolled back transaction value %s, got %s", value2, *key1Value)
	}

}
