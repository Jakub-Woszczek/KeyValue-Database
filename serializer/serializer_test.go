package serializer

import (
    "testing"
)

func TestEncode(t *testing.T) {
    key := []byte("myKey")
    val := []byte("myValue")

    encoded := Encode(key, val)

    k, v, err := Decode(encoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
    if string(k) != "myKey" {
        t.Errorf("expected myKey, got %s", k)
    }
    if string(v) != "myValue" {
        t.Errorf("expected myValue, got %s", v)
    }
}

func TestEemptyBuff(t *testing.T) {
	_, _, err := Decode([]byte{})
	if err == nil {
		t.Fatal("expected error for empty buffer")
	}
}

func TestTooLargeKey(t *testing.T) {
	key := make([]byte, 1<<21) // 2MB key
	val := []byte("value")

	encoded := Encode(key, val)
	_, _, err := Decode(encoded)
	if err == nil {
		t.Fatal("expected error for too large key")
	}
}

func TestTooLargeVal(t *testing.T) {
	key := []byte("key")
	val := make([]byte, 1<<27) // 128MB value

	encoded := Encode(key, val)
	_, _, err := Decode(encoded)
	if err == nil {
		t.Fatal("expected error for too large value")
	}
}