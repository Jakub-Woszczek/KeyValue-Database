package functionaltests

import (
	"bytes"
	"math/rand"
	"testing"

	// "time"

	"github.com/Jakub-Woszczek/kvdb/db"
)

func TestGet_WhenEmpty(t *testing.T) {
	d, _ := db.NewDB()

	val := d.Get([]byte("key"))
	if val != nil {
		t.Fatalf("expected nil, got %v", val)
	}
}

func TestPutAndGet(t *testing.T) {
	d, _ := db.NewDB()

	d.Put([]byte("key"), []byte("value"))

	val := d.Get([]byte("key"))
	if string(val) != "value" {
		t.Fatalf("expected value, got %s", val)
	}
}

func TestGet_NonExistingKey(t *testing.T) {
	d, _ := db.NewDB()

	d.Put([]byte("key"), []byte("value"))

	val := d.Get([]byte("other"))
	if val != nil {
		t.Fatalf("expected nil, got %v", val)
	}
}

func TestPut_OverwriteValue(t *testing.T) {
	d, _ := db.NewDB()

	d.Put([]byte("key"), []byte("value1"))
	d.Put([]byte("key"), []byte("value2"))

	val := d.Get([]byte("key"))
	if string(val) != "value2" {
		t.Fatalf("expected value2, got %s", val)
	}
}

// This test performs a series of random Put and Get operations
// and checks consistency against a reference map.
func TestDB_RandomizedConsistency(t *testing.T) {
	d, err := db.NewDB()
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	defer d.Close(true) // delete WAL file after test

	// reference model
	ref := make(map[string][]byte)

	// deterministic seed for reproducibility
	rng := rand.New(rand.NewSource(42))

	const ops = 1000

	for i := 0; i < ops; i++ {
		key := randomBytes(rng, 10)
		value := randomBytes(rng, 20)

		// 70% writes, 30% reads
		if rng.Float64() < 0.7 {
			err := d.Put(key, value)
			if err != nil {
				t.Fatalf("put failed: %v", err)
			}

			ref[string(key)] = value
		} else {
			dbVal := d.Get(key)
			refVal, exists := ref[string(key)]

			if !exists {
				if dbVal != nil {
					t.Fatalf("expected nil for key %q, got %v", key, dbVal)
				}
			} else {
				if !bytes.Equal(dbVal, refVal) {
					t.Fatalf("value mismatch for key %q: expected %v, got %v",
						key, refVal, dbVal)
				}
			}
		}
	}

	// full verification pass
	for k, v := range ref {
		dbVal := d.Get([]byte(k))
		if !bytes.Equal(dbVal, v) {
			t.Fatalf("final check failed for key %q: expected %v, got %v",
				k, v, dbVal)
		}
	}
}

func randomBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Intn(256))
	}
	return b
}
