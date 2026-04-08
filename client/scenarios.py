from client import KVClient
import time

def run(label, fn):
    print(f"\n{'='*50}")
    print(f"  {label}")
    print('='*50)
    with KVClient() as db:
        fn(db)

def scenario_basic_put_get(db):
    db.put("alpha", "1")
    db.put("beta",  "2")
    db.put("gamma", "3")

    assert db.get("beta") == "2",  "expected '2'"
    assert db.get("zeta") is None, "expected miss"
    print("PUT/GET: OK")

def scenario_delete_tombstone(db):
    db.put("fox",  "red")
    db.put("wolf", "gray")
    db.delete("fox")

    assert db.get("fox")  is None,   "fox should be deleted"
    assert db.get("wolf") == "gray",  "wolf should survive"
    print("DELETE/tombstone: OK")

def scenario_overwrite(db):
    db.put("x", "first")
    db.put("x", "second")  # overwrite

    assert db.get("x") == "second", "expected latest value"
    print("Overwrite: OK")

def scenario_miss(db):
    result = db.get("does_not_exist")
    assert result is None
    print("Miss on empty DB: OK")

def scenario_quit_logic(db):
    db.close()

if __name__ == "__main__":
    run("Basic PUT / GET",          scenario_basic_put_get)
    # run("DELETE + tombstone",       scenario_delete_tombstone)
    run("Overwrite (latest wins)",  scenario_overwrite)
    run("GET miss",                 scenario_miss)
    print("\nAll scenarios passed.")