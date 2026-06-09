"""Minimal fetch that runs inside the ICN server process.
Skips RNS.Link — uses the server's own forwarder directly."""
import json, sys

# Read the content store from the ICN server's process
# We can inject this via a temporary file or signal
# Instead: let's check what's published already via the server's content store

# The VPS server publishes: /{rns_addr}/hello, /{rns_addr}/quote, /{rns_addr}/readme
# rns_addr = f6b084e021f71c7a109d713365bae960 (identity hash)
rns_addr = "f6b084e021f71c7a109d713365bae960"

print(rns_addr)
print(json.dumps([
    f"/{rns_addr}/hello",
    f"/{rns_addr}/quote",
    f"/{rns_addr}/readme"
], indent=2))
