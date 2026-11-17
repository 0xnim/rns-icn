"""Space weather feed: rendering and file handling, with canned NOAA data."""

import importlib.util
import json
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


def test_record_is_compact_json_with_the_documented_fields():
    record = sw.build_record(DATA, NOW)
    assert record == {
        "v": 1, "t": 1794875400, "r": 1, "s": 0, "g": 0, "sfi": 123,
        "kp": 4.67, "kp_t": 1794862800, "a": 30, "wind": 294, "bz": -5, "bt": 7,
        "fc_d": "2026-11-17", "fc": [[25, 5, 5, 1]],
    }
    assert sw.build_record({}, NOW) is None
    assert sw.build_record({"flux": [{"flux": "n/a"}], "kp": "nope"}, NOW) is None


def test_report_time_is_the_newest_measurement():
    assert sw.report_time(DATA, NOW) == 1794875400            # the scales stamp
    later = dict(DATA, wind=[{"proton_speed": 300, "time_tag": "2026-11-17T00:58:00Z"}])
    assert sw.report_time(later, NOW) == 1794877080
    assert sw.report_time({}, NOW) == int(NOW.timestamp())    # NOW is on the hour


def test_render_writes_one_edition_per_collection():
    files = sw.render(DATA, NOW)
    assert set(files) == {
        "swx/now/1794875400", "swx/page/1794875400.mu", "swx/fc/20261117-0030",
        "swx/disc/20261117-0100", "swx/alert/20261116-0628-alert",
    }
    assert json.loads(files["swx/now/1794875400"])["sfi"] == 123
    assert files["swx/now/1794875400"].count("\n") == 1
    assert files["swx/fc/20261117-0030"].endswith("A. Geomagnetic\n")
    assert "\r" not in files["swx/fc/20261117-0030"]
    assert files["swx/alert/20261116-0628-alert"] == "ALERT: new\n"


def test_alert_names_use_the_message_code():
    data = {"alerts": [{"issue_datetime": "2026-11-16 06:28:00.000",
                        "message": "Space Weather Message Code: ALTEF3\r\nSerial Number: 1"}]}
    assert list(sw.render(data, NOW)) == ["swx/alert/20261116-0628-ALTEF3"]


def test_files_are_rewritten_only_when_changed_and_leave_no_temporaries(tmp_path):
    files = sw.render(DATA, NOW)
    assert sw.write_files(tmp_path, files) == 5
    assert sw.write_files(tmp_path, files) == 0
    files["swx/disc/20261117-0100"] = "Changed\n"
    assert sw.write_files(tmp_path, files) == 1
    assert (tmp_path / "swx" / "disc" / "20261117-0100").read_text() == "Changed\n"
    assert not [p for p in tmp_path.rglob("*") if p.name.startswith(".")]


def test_old_editions_are_pruned_per_collection(tmp_path):
    folder = tmp_path / "swx" / "page"
    folder.mkdir(parents=True)
    for stamp in (100, 200, 300, 400, 500):
        (folder / f"{stamp}.mu").write_text("x")
    (folder / ".600.mu.tmp").write_text("x")
    other = tmp_path / "swx" / "now"
    other.mkdir()
    (other / "100").write_text("x")
    assert sw.prune(tmp_path) == 2
    assert sorted(p.name for p in folder.iterdir()) == [
        ".600.mu.tmp", "300.mu", "400.mu", "500.mu"]
    assert (other / "100").exists()
