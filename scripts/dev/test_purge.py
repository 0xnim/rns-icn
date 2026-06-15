import sqlite3
import time

conn = sqlite3.connect("/opt/rns-icn/content_store.db", check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA busy_timeout=5000")

print("=== Test 1: Direct query ===")
h = bytes.fromhex("31D4BC3A05AEAF9CE728C2E1C03B1FBD5C92739E00A9A94D8C00981D2F6E94E0")
row = conn.execute("SELECT name_bytes FROM content WHERE name_hash = ?", (h,)).fetchone()
print("Row:", row)

print("\n=== Test 2: Simulate _purge_expired_internal then query ===")
# Simulate purge
now = int(time.time())
rows = conn.execute(
    "SELECT name_hash FROM content WHERE expires_at IS NOT NULL AND expires_at <= ?",
    (now,)
).fetchall()
print("Purge rows:", rows)
for (name_hash,) in rows:
    conn.execute("DELETE FROM content WHERE name_hash = ?", (name_hash,))
    conn.execute("DELETE FROM name_prefixes WHERE name_hash = ?", (name_hash,))

# Now query again
row = conn.execute("SELECT name_bytes FROM content WHERE name_hash = ?", (h,)).fetchone()
print("Row after purge:", row)

print("\n=== Test 3: Direct query again ===")
row = conn.execute("SELECT name_bytes FROM content WHERE name_hash = ?", (h,)).fetchone()
print("Row:", row)