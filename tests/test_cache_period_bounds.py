"""Full unsigned wire freshness periods survive cache storage without truncation."""
import json
from unittest.mock import patch

import pytest

from rns_icn.content_store import ContentStore
from rns_icn.name import Name
from rns_icn.packet import Data


@pytest.mark.parametrize('period', [None, 0, 60, (1 << 63)-1, 1 << 63, (1 << 64)-1])
def test_period_survives_wire_storage_restart_and_prefix_lookup(tmp_path, period):
    path = str(tmp_path/'cache.db')
    cs = ContentStore(path)
    name = Name(b'p'*16, [b'period'])
    data = Data.new(name, b'content')
    if period is not None:
        data.with_freshness_period(period)
    try:
        with patch('rns_icn.content_store.time.time', return_value=1000):
            cs.insert(name, Data.from_bytes(data.to_bytes()))
        cs.close()
        cs = ContentStore(path)
        with patch('rns_icn.content_store.time.time', return_value=1060):
            for result in (cs.get(name), cs.get_prefix(name)):
                decoded = Data.from_bytes(result.to_bytes())
                assert decoded.metadata.freshness_period == period
                assert decoded.metadata.freshness.fresh is (period is None or period > 60)
        stored = cs._conn.execute('SELECT freshness_period FROM content').fetchone()[0]
        assert stored == (None if period is None else min(period, (1 << 63)-1))
    finally:
        cs.close()


@pytest.mark.parametrize('json_period,column,expected', [
    ('absent', 60, 60), ('absent', None, None),
    (None, 60, None), (60, None, 60), ((1 << 64)-1, (1 << 63)-1, (1 << 64)-1)])
def test_json_is_authoritative_with_legacy_column_fallback(json_period, column, expected):
    cs = ContentStore()
    try:
        meta = {'content_hash': bytes(32).hex(), 'fresh': True}
        if json_period != 'absent':
            meta['freshness_period'] = json_period
        with patch('rns_icn.content_store.time.time', return_value=1100):
            parsed = cs._parse_metadata(json.dumps(meta), inserted_at=1000, freshness_period=column)
        assert parsed.freshness_period == expected
        assert parsed.freshness.fresh is (expected is None or expected > 100)
    finally:
        cs.close()
