"""
API: cob_dt là tham số truy vấn đã validate, không phải chuỗi nối vào SQL.

Bản cũ nhận `cob_dt: str` và chèn `date '{cob_dt}'` thẳng vào SQL (SQL injection),
và hai endpoint join hai bảng cùng có cột cob_dt nên lọc theo ngày báo lỗi
"ambiguous". Trino không cần chạy: execute_query được thay bằng stub ghi lại
SQL và tham số.
"""

import importlib.util
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("fastapi", reason="fastapi không có trong CI env")
pytest.importorskip("trino", reason="trino client không có trong CI env")

from fastapi.testclient import TestClient  # noqa: E402

API_MAIN = Path(__file__).resolve().parents[2] / "api" / "main.py"
spec = importlib.util.spec_from_file_location("api_main_under_test", API_MAIN)
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)

ROWS = {
    "overview": {
        "customer_id": 7,
        "full_name_masked": "Nguyen **",
        "age": 30,
        "gender": "Male",
        "customer_segment": "RETAIL",
        "kyc_status": "VERIFIED",
        "register_date": "2020-01-01",
        "total_accounts": 1,
        "total_cards": 0,
        "total_loans": 0,
        "has_credit_card": False,
        "has_savings": True,
        "has_loan": False,
        "aum_total": 1.0,
        "aum_bucket": "MASS",
        "primary_branch_code": "HCM01",
        "cob_dt": "2026-09-29",
    },
}


@pytest.fixture
def calls(monkeypatch):
    recorded = []

    def fake_execute(sql, params=None):
        recorded.append((sql, params))
        return [ROWS["overview"]]

    monkeypatch.setattr(api, "execute_query", fake_execute)
    return recorded


client = TestClient(api.app)


def test_cob_dt_is_passed_as_parameter(calls):
    response = client.get("/customer/7/overview", params={"cob_dt": "2026-09-29"})
    assert response.status_code == 200
    sql, params = calls[-1]
    assert "2026-09-29" not in sql
    assert "AND cob_dt = ?" in sql
    assert params == (7, date(2026, 9, 29))


def test_injection_attempt_is_rejected_before_sql(calls):
    response = client.get("/customer/7/overview", params={"cob_dt": "2026-09-29' OR '1'='1"})
    assert response.status_code == 422
    assert calls == []


def test_without_cob_dt_only_customer_id_is_bound(calls):
    client.get("/customer/7/overview")
    sql, params = calls[-1]
    assert "cob_dt = ?" not in sql
    assert params == (7,)


@pytest.mark.parametrize(
    ("path", "column"),
    [("/customer/7/risk-score", "c.cob_dt"), ("/customer/7/recommendations", "m.cob_dt")],
)
def test_joined_endpoints_qualify_cob_dt(calls, path, column):
    # Stub trả hàng của overview nên phần hậu xử lý có thể lỗi; điều cần kiểm là SQL đã gửi.
    TestClient(api.app, raise_server_exceptions=False).get(path, params={"cob_dt": "2026-09-29"})
    sql, params = calls[-1]
    assert f"AND {column} = ?" in sql
    assert params == (7, date(2026, 9, 29))
