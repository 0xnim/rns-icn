#!/usr/bin/env python3
"""Space weather feed — NOAA SWPC conditions and forecasts as publishable files.

Fetches a handful of small public products from the NOAA Space Weather
Prediction Center and writes them into a directory that an ICN server
publishes (``publish_dir``):

    spaceweather/now.mu                  current conditions, as a micron page
    spaceweather/forecast.txt            the 3-day forecast, as issued
    spaceweather/discussion.txt          the forecast discussion, as issued
    spaceweather/alerts.txt              alerts and warnings from the last 48 h
    spaceweather/forecast/YYYY-MM-DD.txt one forecast per day, kept for 30 days

Files are only rewritten when their content changes, so nothing is
re-published needlessly. Standard library only.

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
FOLDER = "spaceweather"
KEEP_DAYS = 30
ALERT_WINDOW = timedelta(hours=48)
MAX_ALERTS = 8
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


def render_alerts(alerts, now: datetime) -> str | None:
    if not isinstance(alerts, list):
        return None
    recent = [(when, a) for a in alerts if isinstance(a, dict)
              and (when := _alert_time(a)) is not None and now - when <= ALERT_WINDOW]
    recent.sort(key=lambda pair: pair[0], reverse=True)
    if not recent:
        return "No alerts, watches or warnings in the last 48 hours.\n\n" + CREDIT + "\n"
    parts = [f"Space weather alerts, watches and warnings, last 48 hours "
             f"({min(len(recent), MAX_ALERTS)} of {len(recent)})\n"]
    for _, alert in recent[:MAX_ALERTS]:
        parts.append("-" * 60 + "\n" + _clean(str(alert.get("message", ""))))
    parts.append("-" * 60 + "\n" + CREDIT + "\n")
    return "\n".join(parts)


def render(data: dict, now: datetime | None = None) -> dict[str, str]:
    """Map of relative file path -> content for everything that could be built."""
    now = now or datetime.now(timezone.utc)
    files: dict[str, str] = {}
    page = render_now(data)
    if page:
        files[f"{FOLDER}/now.mu"] = page
    for key in ("forecast", "discussion"):
        text = data.get(key)
        if isinstance(text, str) and text.strip():
            files[f"{FOLDER}/{key}.txt"] = _clean(text)
    forecast = data.get("forecast")
    if isinstance(forecast, str) and forecast.strip():
        day = now.strftime("%Y-%m-%d")
        issued = _issued(forecast)
        if issued:
            try:
                day = datetime.strptime(issued[:11], "%Y %b %d").strftime("%Y-%m-%d")
            except ValueError:
                pass
        files[f"{FOLDER}/forecast/{day}.txt"] = _clean(forecast)
    alerts = render_alerts(data.get("alerts"), now)
    if alerts:
        files[f"{FOLDER}/alerts.txt"] = alerts
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


def prune(root: Path, now: datetime, keep_days: int = KEEP_DAYS) -> int:
    """Remove dated forecasts older than ``keep_days``."""
    removed = 0
    cutoff = (now - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    for path in (root / FOLDER / "forecast").glob("????-??-??.txt"):
        if path.stem < cutoff:
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
    removed = prune(root, now)
    print(f"spaceweather: {len(data)}/{len(SOURCES)} sources, "
          f"{written} file(s) written, {removed} removed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
