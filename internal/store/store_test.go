package store

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

	var delete_test_cases = []struct {
		key string
		ok  bool
	}{
		{"name", true},
		{"alice", false},
	}

	for _, tt := range delete_test_cases {
		t.Run(tt.key, func(t *testing.T) {
			ok := store.Delete(tt.key)
			if ok != tt.ok {
				t.Errorf("got %t expected %t", ok, tt.ok)
			}
		})
	}
}
