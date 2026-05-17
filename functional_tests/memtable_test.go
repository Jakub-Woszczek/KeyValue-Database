package functionaltests

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/Jakub-Woszczek/kvdb/db"
)

func newTestDB(t *testing.T) *db.DB {
	t.Helper()

	tmpDir := t.TempDir()

	d, err := db.NewDB(
		1000000,
		tmpDir,
	)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	t.Cleanup(func() {
		d.Close(true)
	})

	return d
}

func TestGet_WhenEmpty(t *testing.T) {
	d := newTestDB(t)

	val, found, err := d.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if found {
		t.Fatalf("expected not found, got %v", val)
	}
}

func TestPutAndGet(t *testing.T) {
	d := newTestDB(t)

	err := d.Put([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	val, found, err := d.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if !found {
		t.Fatal("expected key to exist")
	}

	if !bytes.Equal(val, []byte("value")) {
		t.Fatalf("expected value, got %q", val)
	}
}

func TestGet_NonExistingKey(t *testing.T) {
	d := newTestDB(t)

	err := d.Put([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	val, found, err := d.Get([]byte("other"))
	if err != nil {
		t.Fatal(err)
	}

	if found {
		t.Fatalf("expected not found, got %v", val)
	}
}

func TestPut_OverwriteValue(t *testing.T) {
	d := newTestDB(t)

	err := d.Put([]byte("key"), []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}

	err = d.Put([]byte("key"), []byte("value2"))
	if err != nil {
		t.Fatal(err)
	}

	val, found, err := d.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if !found {
		t.Fatal("expected key to exist")
	}

	if !bytes.Equal(val, []byte("value2")) {
		t.Fatalf("expected value2, got %q", val)
	}
}

func TestDB_RandomizedConsistency(t *testing.T) {
	d := newTestDB(t)

	ref := make(map[string][]byte)

	rng := rand.New(rand.NewSource(42))

	const ops = 1000

	for i := 0; i < ops; i++ {
		key := randomBytes(rng, 10)
		value := randomBytes(rng, 20)

		if rng.Float64() < 0.7 {
			err := d.Put(key, value)
			if err != nil {
				t.Fatalf("put failed: %v", err)
			}

			ref[string(key)] = value

		} else {
			dbVal, found, err := d.Get(key)
			if err != nil {
				t.Fatal(err)
			}

			refVal, exists := ref[string(key)]

			if !exists {
				if found {
					t.Fatalf("expected not found")
				}
			} else {
				if !found {
					t.Fatalf("expected key %q", key)
				}

				if !bytes.Equal(dbVal, refVal) {
					t.Fatalf(
						"value mismatch for key %q: expected %v got %v",
						key,
						refVal,
						dbVal,
					)
				}
			}
		}
	}

	for k, v := range ref {
		dbVal, found, err := d.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}

		if !found {
			t.Fatalf("missing key %q", k)
		}

		if !bytes.Equal(dbVal, v) {
			t.Fatalf(
				"final check failed for key %q: expected %v got %v",
				k,
				v,
				dbVal,
			)
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
