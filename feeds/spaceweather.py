#!/usr/bin/env python3
"""Space weather feed — NOAA SWPC conditions and forecasts as publishable files.

Fetches a handful of small public products from the NOAA Space Weather
Prediction Center and writes them into a directory that an ICN server
publishes (``publish_dir``). Each folder is a collection; each file in it is
one edition, named so that names sort by time:

    swx/now/<unix time>         current conditions as a compact JSON record
    swx/page/<unix time>.mu     the same conditions as a micron page
    swx/fc/<YYYYMMDD-HHMM>      the 3-day forecast, as issued
    swx/disc/<YYYYMMDD-HHMM>    the forecast discussion, as issued
    swx/alert/<YYYYMMDD-HHMM-code>  one alert, watch or warning

A new edition is a new file; old editions are deleted once a folder holds more
than its limit. Nothing is overwritten, so a copy cached anywhere stays valid.

The ``now`` record (schema version 1) is one line of JSON, about 170 bytes,
small enough to travel in a single Reticulum packet:

    v     schema version                 t     time of the report, unix seconds
    sfi   10.7 cm solar flux, sfu        a     planetary A index
    kp    planetary K index              kp_t  time of that K value
    r s g current NOAA scale levels, 0-5 (radio blackout, radiation, geomagnetic)
    wind  solar wind speed, km/s         bz bt interplanetary field, nT
    fc_d  date of the first outlook day, YYYY-MM-DD
    fc    one row per day from fc_d: [% chance of R1-R2, % chance of R3 or
          higher, % chance of S1 or higher, expected G level]; null if unknown

A value NOAA did not supply is left out. Standard library only.

    python3 spaceweather.py /var/lib/rns-icn/publish
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = "https://services.swpc.noaa.gov/"
SOURCES = {
    "scales": "products/noaa-scales.json",
    "kp": "products/noaa-planetary-k-index.json",
    "flux": "products/summary/10cm-flux.json",
    "wind": "products/summary/solar-wind-speed.json",
    "mag": "products/summary/solar-wind-mag-field.json",
    "alerts": "products/alerts.json",
    "wwv": "text/wwv.txt",
    "forecast": "text/3-day-forecast.txt",
    "discussion": "text/discussion.txt",
}
USER_AGENT = "rns-icn-feed/1 (+https://github.com/0xnim/rns-icn)"
FOLDER = "swx"
# Editions kept per collection. Followers fetch every edition a node holds, so
# these stay small.
KEEP = {"now": 6, "page": 3, "fc": 6, "disc": 3, "alert": 12}
SCHEMA_VERSION = 1
ALERT_WINDOW = timedelta(hours=48)
MAX_ALERTS = 12
CREDIT = "Data: NOAA Space Weather Prediction Center (swpc.noaa.gov)"


def fetch_all(timeout: float = 30.0) -> dict:
    """Download every source; a source that fails is simply absent."""
    out: dict = {}
    for key, path in SOURCES.items():
        try:
            request = urllib.request.Request(BASE + path, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read(2_000_000).decode("utf-8", errors="replace")
            out[key] = json.loads(raw) if path.endswith(".json") else raw
        except Exception as e:
            print(f"spaceweather: {path}: {e}", file=sys.stderr)
    return out


# ── rendering (pure) ──

def _clean(text: str) -> str:
    """Normalise line endings and drop trailing blanks."""
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    return "\n".join(lines).strip() + "\n"


def _literal(text: str) -> str:
    """Micron literal block, so tables keep their alignment."""
    body = "\n".join(("\\" + line) if line.startswith("`=") else line
                     for line in text.rstrip("\n").split("\n"))
    return f"`=\n{body}\n`=\n"


def _scale(entry: dict | None, letter: str) -> str:
    try:
        level = int((entry or {}).get(letter, {}).get("Scale") or 0)
    except (TypeError, ValueError):
        return "?"
    return f"{letter}{level}" if level else "none"


def _percent(value) -> str:
    return f"{value}%" if value not in (None, "") else "-"


def _kp_bar(kp: float) -> str:
    return "#" * max(1, round(kp * 2)) if kp > 0 else "."


def _issued(text: str) -> str | None:
    for line in text.split("\n")[:5]:
        if line.startswith(":Issued:"):
            return line.split(":Issued:", 1)[1].strip()
    return None


def _wwv_body(text: str) -> str:
    return "\n".join(
        line for line in _clean(text).split("\n")
        if line and not line.startswith((":", "#"))
    )


def render_now(data: dict) -> str | None:
    """The current-conditions page, or None when there is nothing to show."""
    out: list[str] = [">Space weather now\n"]
    have = False

    scales = data.get("scales")
    if isinstance(scales, dict) and "0" in scales:
        have = True
        now = scales["0"]
        out.append(f"As of {now.get('DateStamp', '?')} {str(now.get('TimeStamp', ''))[:5]} UTC\n\n")
        rows = [
            "                      now    past 24 h",
            f"Radio blackouts  (R)  {_scale(now, 'R'):<6} {_scale(scales.get('-1'), 'R')}",
            f"Radiation storms (S)  {_scale(now, 'S'):<6} {_scale(scales.get('-1'), 'S')}",
            f"Geomagnetic storm (G) {_scale(now, 'G'):<6} {_scale(scales.get('-1'), 'G')}",
        ]
        out.append(_literal("\n".join(rows)))

    facts: list[str] = []
    flux = data.get("flux")
    if isinstance(flux, list) and flux and "flux" in flux[-1]:
        facts.append(f"Solar flux (10.7 cm)   {flux[-1]['flux']} sfu")
    kp = [row for row in data.get("kp") or [] if isinstance(row, dict) and "Kp" in row]
    if kp:
        facts.append(f"Planetary K index      {float(kp[-1]['Kp']):.2f}"
                     f"   (at {kp[-1].get('time_tag', '')[11:16]} UTC)")
        if "a_running" in kp[-1]:
            facts.append(f"Planetary A index      {kp[-1]['a_running']}")
    wind = data.get("wind")
    if isinstance(wind, list) and wind and wind[-1].get("proton_speed") is not None:
        facts.append(f"Solar wind speed       {wind[-1]['proton_speed']} km/s")
    mag = data.get("mag")
    if isinstance(mag, list) and mag and mag[-1].get("bz_gsm") is not None:
        facts.append(f"Magnetic field Bz      {mag[-1]['bz_gsm']} nT   (Bt {mag[-1].get('bt', '?')} nT)")
    if facts:
        have = True
        out.append("\n>>Indices\n" + _literal("\n".join(facts)))

    if kp:
        rows = [f"{row.get('time_tag', '')[5:16].replace('T', ' ')}  "
                f"{float(row['Kp']):4.2f}  {_kp_bar(float(row['Kp']))}" for row in kp[-8:]]
        out.append("\n>>K index, last 24 hours (UTC)\n" + _literal("\n".join(rows)))
        out.append("K of 5 or more is a geomagnetic storm (G1 and up).\n")

    if isinstance(scales, dict) and any(day in scales for day in "123"):
        rows = ["            R1-R2   R3+     S1+     G"]
        for day in "123":
            entry = scales.get(day)
            if not isinstance(entry, dict):
                continue
            r, s = entry.get("R", {}), entry.get("S", {})
            rows.append(
                f"{entry.get('DateStamp', '?'):<11} {_percent(r.get('MinorProb')):<7} "
                f"{_percent(r.get('MajorProb')):<7} {_percent(s.get('Prob')):<7} "
                f"{_scale(entry, 'G')}"
            )
        out.append("\n>>Next three days\n" + _literal("\n".join(rows)))

    wwv = data.get("wwv")
    if isinstance(wwv, str) and _wwv_body(wwv):
        have = True
        out.append("\n>>Summary\n" + _literal(_wwv_body(wwv)))

    if not have:
        return None
    out.append("\n-\n`*" + CREDIT + "`*\n")
    return "".join(out)


def _alert_time(alert: dict) -> datetime | None:
    try:
        return datetime.strptime(alert["issue_datetime"][:19], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except (KeyError, ValueError, TypeError):
        return None


def _level(entry: dict | None, letter: str) -> int | None:
    try:
        return int((entry or {}).get(letter, {}).get("Scale") or 0)
    except (TypeError, ValueError):
        return None


def _number(value) -> int | float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number == int(number) else round(number, 2)


def _epoch(text: str, formats: tuple[str, ...]) -> int | None:
    for fmt in formats:
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).timestamp())
        except (TypeError, ValueError):
            continue
    return None


def report_time(data: dict, now: datetime) -> int:
    """Time of the newest measurement in the report, else the current hour.

    Editions are named by this, so it has to move whenever any value can have.
    """
    times: list[int] = []
    scales = data.get("scales")
    if isinstance(scales, dict) and isinstance(scales.get("0"), dict):
        stamp = f"{scales['0'].get('DateStamp', '')} {scales['0'].get('TimeStamp', '')}"
        times.append(_epoch(stamp, ("%Y-%m-%d %H:%M:%S",)) or 0)
    for key in ("wind", "mag", "kp", "flux"):
        rows = data.get(key)
        if isinstance(rows, list) and rows and isinstance(rows[-1], dict):
            tag = str(rows[-1].get("time_tag", ""))[:19]
            times.append(_epoch(tag, ("%Y-%m-%dT%H:%M:%S",)) or 0)
    newest = max(times, default=0)
    if newest:
        return newest
    return int(now.replace(minute=0, second=0, microsecond=0).timestamp())


def build_record(data: dict, now: datetime) -> dict | None:
    """The machine-readable conditions record, or None if nothing is known."""
    record: dict = {}
    scales = data.get("scales") if isinstance(data.get("scales"), dict) else {}
    for key, letter in (("r", "R"), ("s", "S"), ("g", "G")):
        if isinstance(scales.get("0"), dict):
            level = _level(scales["0"], letter)
            if level is not None:
                record[key] = level
    flux = data.get("flux")
    if isinstance(flux, list) and flux and isinstance(flux[-1], dict):
        record["sfi"] = _number(flux[-1].get("flux"))
    kp = [row for row in data.get("kp") or [] if isinstance(row, dict) and "Kp" in row]
    if kp:
        record["kp"] = _number(kp[-1]["Kp"])
        record["kp_t"] = _epoch(str(kp[-1].get("time_tag", ""))[:19], ("%Y-%m-%dT%H:%M:%S",))
        record["a"] = _number(kp[-1].get("a_running"))
    wind = data.get("wind")
    if isinstance(wind, list) and wind and isinstance(wind[-1], dict):
        record["wind"] = _number(wind[-1].get("proton_speed"))
    mag = data.get("mag")
    if isinstance(mag, list) and mag and isinstance(mag[-1], dict):
        record["bz"] = _number(mag[-1].get("bz_gsm"))
        record["bt"] = _number(mag[-1].get("bt"))
    outlook = []
    for day in "123":
        entry = scales.get(day)
        if not isinstance(entry, dict):
            continue
        r, s = entry.get("R") or {}, entry.get("S") or {}
        if not outlook:
            record["fc_d"] = entry.get("DateStamp")
        outlook.append([_number(r.get("MinorProb")), _number(r.get("MajorProb")),
                        _number(s.get("Prob")), _level(entry, "G")])
    if outlook:
        record["fc"] = outlook
    record = {k: v for k, v in record.items() if v is not None}
    if not record:
        return None
    return {"v": SCHEMA_VERSION, "t": report_time(data, now), **record}


def _issue_stamp(text: str, now: datetime) -> str:
    """``YYYYMMDD-HHMM`` from a product's ``:Issued:`` line, else the current time."""
    issued = _issued(text)
    if issued:
        try:
            return datetime.strptime(issued[:16], "%Y %b %d %H%M").strftime("%Y%m%d-%H%M")
        except ValueError:
            pass
    return now.strftime("%Y%m%d-%H%M")


def _alert_code(alert: dict) -> str:
    for line in str(alert.get("message", "")).split("\n")[:3]:
        if "Message Code:" in line:
            code = "".join(ch for ch in line.split("Message Code:", 1)[1] if ch.isalnum())
            if code:
                return code[:8]
    code = "".join(ch for ch in str(alert.get("product_id", "")) if ch.isalnum())
    return code[:8] or "alert"


def render(data: dict, now: datetime | None = None) -> dict[str, str]:
    """Map of relative file path -> content for everything that could be built."""
    now = now or datetime.now(timezone.utc)
    files: dict[str, str] = {}
    record = build_record(data, now)
    if record:
        files[f"{FOLDER}/now/{record['t']}"] = json.dumps(record, separators=(",", ":")) + "\n"
    page = render_now(data)
    if page:
        files[f"{FOLDER}/page/{report_time(data, now)}.mu"] = page
    for key, folder in (("forecast", "fc"), ("discussion", "disc")):
        text = data.get(key)
        if isinstance(text, str) and text.strip():
            files[f"{FOLDER}/{folder}/{_issue_stamp(text, now)}"] = _clean(text)
    alerts = data.get("alerts")
    if isinstance(alerts, list):
        recent = [(when, a) for a in alerts if isinstance(a, dict)
                  and (when := _alert_time(a)) is not None and now - when <= ALERT_WINDOW]
        recent.sort(key=lambda pair: pair[0], reverse=True)
        for when, alert in recent[:MAX_ALERTS]:
            name = f"{when.strftime('%Y%m%d-%H%M')}-{_alert_code(alert)}"
            files[f"{FOLDER}/alert/{name}"] = _clean(str(alert.get("message", "")))
    return files


# ── writing ──

def write_files(root: Path, files: dict[str, str]) -> int:
    """Write changed files atomically. Returns how many were written."""
    written = 0
    for relative, content in files.items():
        path = root / relative
        encoded = content.encode("utf-8")
        try:
            if path.read_bytes() == encoded:
                continue
        except OSError:
            pass
        path.parent.mkdir(parents=True, exist_ok=True)
        # Dot-prefixed so a publisher scanning the directory ignores it.
        temporary = path.with_name("." + path.name + ".tmp")
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
        written += 1
    return written


def prune(root: Path) -> int:
    """Delete the oldest editions beyond each collection's limit.

    Edition names sort by time, so the newest are the last ones by name.
    """
    removed = 0
    for folder, keep in KEEP.items():
        directory = root / FOLDER / folder
        if not directory.is_dir():
            continue
        editions = sorted(p for p in directory.iterdir()
                          if p.is_file() and not p.name.startswith("."))
        for path in editions[:-keep]:
            path.unlink()
            removed += 1
    return removed


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    root = Path(argv[1])
    now = datetime.now(timezone.utc)
    data = fetch_all()
    if not data:
        print("spaceweather: nothing could be fetched; leaving files as they are",
              file=sys.stderr)
        return 1
    written = write_files(root, render(data, now))
    removed = prune(root)
    print(f"spaceweather: {len(data)}/{len(SOURCES)} sources, "
          f"{written} file(s) written, {removed} removed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
