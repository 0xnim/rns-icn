"""Check local backbone connections."""
import RNS, time
RNS.Reticulum()
time.sleep(5)
for iface in RNS.Transport.interfaces:
    name = getattr(iface, 'name', '?')
    if 'VPS' not in name:
        if hasattr(iface, 'target_host'):
            print(f"{name}: {iface.target_host}:{getattr(iface,'target_port','?')} online={getattr(iface,'online','?')}")
