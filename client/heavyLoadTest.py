import random
import time
from client import KVClient

OPS = 60_000 
KEY_SPACE = 10_000

def heavy_test(db):
    rnd = random.Random(42)

    keys = [f"k{i}" for i in range(KEY_SPACE)]

    # ground truth state
    truth = {}

    puts = 0
    gets = 0
    mismatches = 0
    misses = 0

    start = time.time()

    for i in range(OPS):
        op = rnd.random()
        key = rnd.choice(keys)

        # 60% PUT
        if op < 0.6:
            value = f"v{rnd.randint(1, 10_000)}"
            db.put(key, value)
            truth[key] = value
            puts += 1

        # 40% GET
        else:
            expected = truth.get(key)
            actual = db.get(key)

            if actual is None:
                misses += 1

            # correctness check
            if actual != expected:
                mismatches += 1
                print(f"\n❌ MISMATCH at op {i}")
                print(f"key={key}")
                print(f"expected={expected}")
                print(f"actual={actual}")
                raise RuntimeError("Data inconsistency detected")

            gets += 1

        # progress log
        if i % 10_000 == 0 and i > 0:
            elapsed = time.time() - start
            print(f"[{i}/{OPS}] {elapsed:.2f}s | PUT={puts} GET={gets} MISS={misses}")

    elapsed = time.time() - start

    print("\n=== STRESS TEST COMPLETE ===")
    print(f"Total ops: {OPS}")
    print(f"PUTs: {puts}, GETs: {gets}, MISSES: {misses}")
    print(f"Time: {elapsed:.2f}s")
    print(f"Ops/sec: {OPS/elapsed:.2f}")
    print("Consistency: OK")

if __name__ == "__main__": 
    with KVClient() as db: 
        heavy_test(db)