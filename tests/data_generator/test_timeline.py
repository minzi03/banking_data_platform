"""
Ngày sinh ra phải neo vào `as_of` — ngày `cob_dt` sẽ nạp
========================================================

Trước đây cửa sổ giao dịch viết cứng 2025-06-01 → 2026-08-01, không liên quan
gì tới `cob_dt`. Snapshot 2026-09-22 có giao dịch mới nhất 2026-08-03 — cách
50 ngày — nên mọi KPI "30 ngày gần nhất" của Gold bằng 0 và không khách nào ở
mức churn Active. Và helper seasonal đẩy ngày VƯỢT mốc cuối tới 6 ngày.

Chạy: pytest tests/data_generator/test_timeline.py -v
"""

from __future__ import annotations

import random
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATORS = REPO_ROOT / "data_generator" / "generators"
sys.path.insert(0, str(REPO_ROOT / "data_generator"))

from generators import card_crm, core_banking, digital_banking, timeline  # noqa: E402

CONFIG = yaml.safe_load((REPO_ROOT / "data_generator" / "config" / "seed_config.yaml").read_text(encoding="utf-8"))

# Timestamp lưu ở UTC; ngày nghiệp vụ là giờ Việt Nam (ADR-0004). "Mới nhất là
# as_of" phải đúng theo NGÀY NGHIỆP VỤ — 18:00 UTC ngày as_of là 01:00 ngày sau.
ICT = timedelta(hours=7)


def _business_date(ts: datetime) -> date:
    return (ts + ICT).date()


# Vị trí txn_date trong tuple mỗi generator trả về (đo từ generator, không phải
# từ DDL — phần tử cuối là last_updated = now(), không phải ngày nghiệp vụ).
TXN_DATE_INDEX = {"account": 3, "card": 3, "online": 12}


@pytest.fixture(autouse=True)
def _reset_timeline():
    yield
    timeline.set_as_of(timeline.REFERENCE_AS_OF)


def _txn_dates(as_of: date, count: int = 4000) -> dict[str, list[datetime]]:
    timeline.set_as_of(as_of)
    random.seed(20260927)
    rows = {
        "account": core_banking.generate_txn_account(
            count, CONFIG["core_banking"]["txn_account"], [1, 2, 3], {1: 1, 2: 2, 3: 3}
        ),
        "card": card_crm.generate_card_txn(count, CONFIG["card_crm"]["card_txn"], [(1, 1, "DEBIT", "ACTIVE")]),
        "online": digital_banking.generate_online_transactions(
            count, CONFIG["digital_banking"]["online_transaction"], [1], [1], [1]
        ),
    }
    return {kind: [r[TXN_DATE_INDEX[kind]] for r in rs] for kind, rs in rows.items()}


def test_shift_moves_literals_by_the_as_of_offset():
    timeline.set_as_of(date(2026, 9, 22))
    assert timeline.current_as_of() == date(2026, 9, 22)
    assert timeline.shift("2026-08-01") == "2026-09-22"
    assert timeline.shift("2025-06-01") == "2025-07-23"


def test_reference_as_of_is_the_identity():
    timeline.set_as_of(timeline.REFERENCE_AS_OF)
    assert timeline.shift("2025-06-01") == "2025-06-01"


@pytest.mark.parametrize("as_of", [date(2026, 1, 1), date(2026, 9, 22), date(2026, 8, 1)])
def test_newest_transaction_is_at_as_of_never_after(as_of):
    """Giao dịch mới nhất rơi sát `as_of` — và không có giao dịch nào sau nó, theo ngày nghiệp vụ."""
    for kind, dates in _txn_dates(as_of).items():
        newest = max(_business_date(d) for d in dates)
        assert newest <= as_of, f"{kind}: giao dịch {newest} nằm SAU as_of {as_of}"
        assert newest >= as_of - timedelta(days=7), f"{kind}: giao dịch mới nhất {newest} cách as_of quá xa"


def test_last_30_days_are_populated():
    """Đúng triệu chứng cũ: cửa sổ 30 ngày trước as_of phải có giao dịch."""
    as_of = date(2026, 9, 22)
    for kind, dates in _txn_dates(as_of).items():
        recent = sum(1 for d in dates if as_of - timedelta(days=30) <= _business_date(d) <= as_of)
        assert recent > 0, f"{kind}: không có giao dịch nào trong 30 ngày trước as_of"


def test_no_generator_keeps_an_unanchored_date_literal():
    """Literal ngày mới thêm vào mà quên shift() sẽ lại trôi khỏi cob_dt."""
    loose = []
    for name in ("core_banking", "card_crm", "digital_banking"):
        for n, line in enumerate((GENERATORS / f"{name}.py").read_text(encoding="utf-8").splitlines(), 1):
            code = line.split("#", 1)[0]
            for m in re.finditer(r'"(?:19|20)\d{2}-\d{2}-\d{2}"', code):
                if not code[: m.start()].endswith("shift("):
                    loose.append(f"{name}.py:{n}: {line.strip()}")
            if re.search(r"datetime\(20\d{2},", code) and "shift_dt(" not in code:
                loose.append(f"{name}.py:{n}: {line.strip()}")
    assert not loose, "Ngày literal chưa neo vào timeline.shift():\n" + "\n".join(loose)


def test_crm_interactions_reach_as_of():
    """KPI `interaction_count_90d` của Customer 360 cần CRM trong 90 ngày trước as_of.

    Trước đây CRM dừng 7 tháng trước giao dịch mới nhất — sau khi neo as_of, cửa sổ
    90 ngày vẫn rỗng (đo trên stack 2026-09-27: 0 / 10.000 khách).
    """
    as_of = date(2026, 9, 22)
    timeline.set_as_of(as_of)
    random.seed(20260927)
    rows = card_crm.generate_crm_interactions(3000, CONFIG["card_crm"]["crm_interaction"], [1, 2, 3])
    days = [_business_date(r[2]) for r in rows]
    assert max(days) <= as_of
    assert max(days) >= as_of - timedelta(days=7)
    assert any(as_of - timedelta(days=90) <= d for d in days)


def test_full_size_merchant_generation_terminates():
    """2.000 merchant từng treo vĩnh viễn: vòng `while` chỉ đổi chữ A–E của một cặp đã cạn.

    Chạy trong tiến trình con có timeout: nếu lỗi quay lại, test ĐỎ thay vì treo cả suite.
    """
    code = (
        "import random, yaml; random.seed(7)\n"
        "from generators import digital_banking as db\n"
        "cfg = yaml.safe_load(open('config/seed_config.yaml', encoding='utf-8'))['digital_banking']\n"
        "mcc = [r[0] for r in db.generate_mcc_codes(cfg['mcc_code'])]\n"
        "m = db.generate_merchants(cfg['merchant']['row_count'], cfg['merchant'], mcc, cfg['location'].get('cities'))\n"
        "print(len(m), len({r[1] for r in m}))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT / "data_generator",
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    total, unique = map(int, out.stdout.split())
    assert total == unique == CONFIG["digital_banking"]["merchant"]["row_count"]
