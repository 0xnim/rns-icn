"""Space weather feed: rendering and file handling, with canned NOAA data."""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "spaceweather", Path(__file__).parent.parent / "feeds" / "spaceweather.py"
)
sw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sw)

NOW = datetime(2026, 11, 17, 1, 0, tzinfo=timezone.utc)
FORECAST = ":Product: 3-Day Forecast\r\n:Issued: 2026 Nov 17 0030 UTC\r\n#\r\nA. Geomagnetic   \r\n"
DATA = {
    "scales": {
        "0": {"DateStamp": "2026-11-17", "TimeStamp": "00:30:00",
              "R": {"Scale": "1"}, "S": {"Scale": "0"}, "G": {"Scale": None}},
        "-1": {"R": {"Scale": "0"}, "S": {"Scale": "0"}, "G": {"Scale": "2"}},
        "1": {"DateStamp": "2026-11-17", "R": {"MinorProb": "25", "MajorProb": "5"},
              "S": {"Prob": "5"}, "G": {"Scale": "1"}},
    },
    "kp": [{"time_tag": "2026-11-16T18:00:00", "Kp": 0.0},
           {"time_tag": "2026-11-16T21:00:00", "Kp": 4.67, "a_running": 30}],
    "flux": [{"flux": 123, "time_tag": "2026-11-16T20:00:00"}],
    "wind": [{"proton_speed": 294}],
    "mag": [{"bt": 7, "bz_gsm": -5}],
    "wwv": ":Product: wwv\n# header\nSolar flux 123.\n\n`=tricky line\n",
    "forecast": FORECAST,
    "discussion": "Discussion text\n",
    "alerts": [
        {"issue_datetime": "2026-11-16 06:28:00.000", "message": "ALERT: new\r\n"},
        {"issue_datetime": "2026-11-10 06:28:00.000", "message": "ALERT: old"},
        {"issue_datetime": "garbage", "message": "ALERT: undated"},
    ],
}


def test_now_page_shows_scales_indices_and_forecast():
    page = sw.render_now(DATA)
    assert page.startswith(">Space weather now\n")
    assert "As of 2026-11-17 00:30 UTC" in page
    assert "Radio blackouts  (R)  R1     none" in page
    assert "Geomagnetic storm (G) none   G2" in page
    assert "Solar flux (10.7 cm)   123 sfu" in page
    assert "Planetary K index      4.67   (at 21:00 UTC)" in page
    assert "Magnetic field Bz      -5 nT   (Bt 7 nT)" in page
    assert "11-16 21:00  4.67  #########" in page
    assert "11-16 18:00  0.00  ." in page
    assert "2026-11-17  25%     5%      5%      G1" in page
    assert "Solar flux 123." in page and ":Product" not in page
    assert "\\`=tricky line" in page          # cannot close the literal block
    assert page.count("`=\n") % 2 == 0
    assert "NOAA Space Weather Prediction Center" in page


def test_now_page_survives_missing_and_malformed_sources():
    assert sw.render_now({}) is None
    assert sw.render_now({"scales": [], "kp": "nope", "flux": []}) is None
    page = sw.render_now({"flux": [{"flux": 99}]})
    assert "99 sfu" in page and "K index" not in page


def test_render_builds_every_file_and_dates_the_forecast_from_its_header():
    files = sw.render(DATA, NOW)
    assert set(files) == {
        "spaceweather/now.mu", "spaceweather/forecast.txt", "spaceweather/discussion.txt",
        "spaceweather/forecast/2026-11-17.txt", "spaceweather/alerts.txt",
    }
    assert files["spaceweather/forecast.txt"].endswith("A. Geomagnetic\n")
    assert "\r" not in files["spaceweather/forecast.txt"]
    alerts = files["spaceweather/alerts.txt"]
    assert "ALERT: new" in alerts and "old" not in alerts and "undated" not in alerts


def test_alerts_file_says_when_there_are_none():
    assert "No alerts" in sw.render_alerts([], NOW)
    assert sw.render_alerts(None, NOW) is None


def test_files_are_rewritten_only_when_changed_and_leave_no_temporaries(tmp_path):
    files = sw.render(DATA, NOW)
    assert sw.write_files(tmp_path, files) == 5
    assert sw.write_files(tmp_path, files) == 0
    files["spaceweather/discussion.txt"] = "Changed\n"
    assert sw.write_files(tmp_path, files) == 1
    assert (tmp_path / "spaceweather" / "discussion.txt").read_text() == "Changed\n"
    assert not [p for p in tmp_path.rglob("*") if p.name.startswith(".")]


def test_old_dated_forecasts_are_pruned(tmp_path):
    folder = tmp_path / "spaceweather" / "forecast"
    folder.mkdir(parents=True)
    for day in ("2026-10-01", "2026-11-01", "2026-11-17"):
        (folder / f"{day}.txt").write_text("x")
    (folder / "notes.txt").write_text("x")
    assert sw.prune(tmp_path, NOW) == 1
    assert sorted(p.name for p in folder.iterdir()) == [
        "2026-11-01.txt", "2026-11-17.txt", "notes.txt"]
