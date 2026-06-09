#!/bin/bash
export RNS_CONFIG=/etc/rnsd-icn
export HOME=/root
cd /opt/rns-icn
exec /opt/rns-icn-venv/bin/python3 -u /opt/rns-icn/rns_transport_icn.py
