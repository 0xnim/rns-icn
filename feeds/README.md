# Feeds

Small scripts that write files into an ICN server's `publish_dir`
(see `icn.toml.example`). The server publishes whatever appears there.

## Space weather

`spaceweather.py` fetches current conditions, the 3-day forecast, the forecast
discussion and recent alerts from the NOAA Space Weather Prediction Center and
writes them under `spaceweather/`. It uses only the standard library and
rewrites a file only when its content has changed.

```
python3 feeds/spaceweather.py /var/lib/rns-icn/publish
```

`icn-feed-spaceweather.service` and `.timer` run it hourly under systemd. Copy
the script to `/opt/rns-icn/feeds/`, the units to `/etc/systemd/system/`, then
`systemctl enable --now icn-feed-spaceweather.timer`.
