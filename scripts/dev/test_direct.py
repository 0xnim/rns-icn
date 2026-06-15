import sqlite3

conn = sqlite3.connect("/opt/rns-icn/content_store.db", check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")

h1 = bytes.fromhex("31D4BC3A05AEAF9CE728C2E1C03B1FBD5C92739E00A9A94D8C00981D2F6E94E0")

cursor = conn.execute(
    "SELECT name_bytes, content_bytes, metadata_json FROM content WHERE name_hash = ?",
    (h1,)
)
print("Cursor:", cursor)
row = cursor.fetchone()
print("Row:", row)
if row:
    print("  name_bytes:", row[0].hex()[:40])
    print("  content_bytes:", row[1])
    print("  metadata_json:", row[2][:80])
else:
    print("NO ROW RETURNED")
    for r in conn.execute("SELECT hex(name_hash) FROM content"):
        print("  DB has:", r)