import sqlite3
conn = sqlite3.connect("/opt/rns-icn/content_store.db")

h1 = bytes.fromhex("31D4BC3A05AEAF9CE728C2E1C03B1FBD5C92739E00A9A94D8C00981D2F6E94E0")
h2 = bytes.fromhex("E082D782C511199748D9CD5CEF0BADB179C7BA359F669D726CE41021B13456C9")

print("Testing h1 (manifest):")
row = conn.execute("SELECT hex(name_hash) FROM content WHERE name_hash = ?", (h1,)).fetchone()
print("  Result:", row)

print("Testing h2 (test/hello):")
row = conn.execute("SELECT hex(name_hash) FROM content WHERE name_hash = ?", (h2,)).fetchone()
print("  Result:", row)

print("Direct query:")
for row in conn.execute("SELECT hex(name_hash) FROM content"):
    print(" ", row)