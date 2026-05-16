package main

import "testing"

func TestStoreSetGet(t *testing.T) {
	store := NewStore()

	store.Set("name", "alice")

	value, ok := store.Get("name")

	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "alice" {
		t.Fatalf("expected alice, got %s", value)
	}
}

func TestStoreDelete(t *testing.T) {
	store := NewStore()

	store.Set("name", "alice")

	ok := store.Delete("name")

	if !ok {
		t.Fatal("expected delete to succeed")
	}

	_, exists := store.Get("name")

	if exists {
		t.Fatal("expected key to be deleted")
	}
}
