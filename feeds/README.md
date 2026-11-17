# Feeds

Small scripts that write files into an ICN server's `publish_dir`
(see `icn.toml.example`). The server publishes whatever appears there.

## Space weather

`spaceweather.py` fetches current conditions, the 3-day forecast, the forecast
discussion and recent alerts from the NOAA Space Weather Prediction Center and
writes them under `swx/`. Each folder is a collection and each file one
edition; old editions are deleted, nothing is overwritten.

| Collection | Edition |
|---|---|
| `swx/now` | Current conditions as one line of JSON, about 170 bytes (fields are documented at the top of the script) |
| `swx/page` | The same conditions as a micron page |
| `swx/fc` | The 3-day forecast, as issued |
| `swx/disc` | The forecast discussion, as issued |
| `swx/alert` | One alert, watch or warning |

A program reads the current conditions by asking for the latest item under
`/<node>/swx/now`, or subscribes to that prefix to be sent each new one.
It uses only the standard library.

```
python3 feeds/spaceweather.py /var/lib/rns-icn/publish
```

`icn-feed-spaceweather.service` and `.timer` run it hourly under systemd. Copy
the script to `/opt/rns-icn/feeds/`, the units to `/etc/systemd/system/`, then
`systemctl enable --now icn-feed-spaceweather.timer`.
