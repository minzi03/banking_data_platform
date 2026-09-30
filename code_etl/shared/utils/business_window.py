"""
Cửa sổ ngày nghiệp vụ cho các bảng nạp tăng dần (ADR-0018).

Ba bảng giao dịch (Bronze core_txn_account / core_card_txn / core_online_transaction và
Silver fact tương ứng) partition theo NGÀY NGHIỆP VỤ của giao dịch. Mỗi lượt hằng ngày
nạp đúng cửa sổ [cob_dt, cob_dt + 1); lần nạp đầu mở rộng cận dưới bằng backfill_from.
"""

from datetime import date, timedelta

# Nạp toàn bộ lịch sử có ở nguồn (bootstrap / stack mới).
BOOTSTRAP_FROM = "1900-01-01"


def load_window(cob_dt: str, backfill_from: str | None = None) -> dict:
    """Biến template {window_start, window_end}: khoảng nửa mở theo ngày."""
    start = date.fromisoformat(backfill_from or cob_dt)
    end = date.fromisoformat(cob_dt) + timedelta(days=1)
    if start >= end:
        raise ValueError(f"backfill_from={backfill_from} phải <= cob_dt={cob_dt}")
    return {"window_start": start.isoformat(), "window_end": end.isoformat()}
