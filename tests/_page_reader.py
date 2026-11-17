"""Page-browser subprocess for the page front end integration test.

Does what a NomadNet-compatible browser does: resolve a path to the node
destination, open a Link, and request pages. Prints one ``PAGES <json>`` line
with the index page, the first item page linked from it, and the producer page
linked from that item's breadcrumb.

Not a test module. Invoked as::

    python tests/_page_reader.py <configdir> <port> <node_dest_hexhash>
"""

import json
import os
import re
import sys
import threading
import time

import RNS

LINK = re.compile(r"`\[[^`]*`:(/page/\w+\.mu)`(\w)=([0-9a-f]+)\]")


def main() -> None:
    configdir, port, dest_hex = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    os.makedirs(configdir, exist_ok=True)
    with open(os.path.join(configdir, "config"), "w") as f:
        f.write(f"""[reticulum]
  enable_transport = No
  share_instance = No
  panic_on_interface_error = No

[logging]
  loglevel = 2

[interfaces]
  [[TCP Client]]
    type = TCPClientInterface
    interface_enabled = yes
    target_host = 127.0.0.1
    target_port = {port}
""")
    RNS.Reticulum(configdir=configdir)
    dest_hash = bytes.fromhex(dest_hex)

    deadline = time.time() + 30
    while not RNS.Transport.has_path(dest_hash) and time.time() < deadline:
        RNS.Transport.request_path(dest_hash)
        time.sleep(1)
    identity = RNS.Identity.recall(dest_hash)
    if identity is None:
        print("PAGES " + json.dumps({"error": "no path to node"}), flush=True)
        return

    dest = RNS.Destination(
        identity, RNS.Destination.OUT, RNS.Destination.SINGLE, "nomadnetwork", "node"
    )
    established = threading.Event()
    link = RNS.Link(dest, established_callback=lambda _l: established.set())
    if not established.wait(30):
        print("PAGES " + json.dumps({"error": "link not established"}), flush=True)
        return

    def request(path: str, fields: dict | None = None) -> str | None:
        done = threading.Event()
        box: list = [None]

        def ok(receipt):
            box[0] = receipt.response
            done.set()

        link.request(path, data=fields, response_callback=ok,
                     failed_callback=lambda _r: done.set(), timeout=30)
        done.wait(35)
        value = box[0]
        return value.decode("utf-8") if isinstance(value, bytes) else value

    result: dict = {"index": request("/page/index.mu")}
    # The index lists the node's own items directly; the item page's
    # breadcrumb leads to the producer page.
    for key in ("item", "producer"):
        previous = result["index"] if key == "item" else result["item"]
        for match in LINK.finditer(previous or ""):
            if match.group(1) == f"/page/{key}.mu":
                break
        else:
            match = None
        result[key] = (
            request(match.group(1), {f"var_{match.group(2)}": match.group(3)})
            if match else None
        )
    link.teardown()
    print("PAGES " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
