"""
Executable regression: chiều điểm RFM, ngày nghiệp vụ ICT, và dim "as-of cob_dt".

Chạy SQL THẬT của rfm_segment.yml và customer_360.yml trên bảng Silver dựng từ
DDL THẬT (docker/init_iceberg/02_ddl_silver.sql), nên đổi tên/kiểu cột trong DDL
mà không sửa Gold thì test này fail.

Lỗi mà file này khoá lại:
  1. NTILE sắp ngược → khách TỐT NHẤT nhận điểm 1 và rơi vào "Hibernating", khách
     tệ nhất thành "Champions" (kế thừa từ template khoá học). KPI dictionary: 5 = tốt nhất.
  2. days_since_last_txn / recency tính bằng CAST(ts AS DATE) trần → ngày UTC,
     lệch 1 ngày với giao dịch sau 17:00 UTC (00:00 ICT).
  3. Gold chọn dim bằng is_current = 1 → chạy lại một cob_dt cũ dùng dim của HÔM
     NAY. Đúng phải là version hiệu lực tại cob_dt.

Marked `integration` (cần pyspark), chạy trong job "Gold Spark Regression" của CI.
"""

import os
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

pyspark = pytest.importorskip("pyspark.sql.functions", reason="pyspark không có trong CI env")

from pyspark.sql import SparkSession  # noqa: E402

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "code_etl" / "gold"
SILVER_DDL = PROJECT_ROOT / "docker" / "init_iceberg" / "02_ddl_silver.sql"
sys.path.insert(0, str(PROJECT_ROOT / "code_etl" / "shared"))

from utils.sql_renderer import render_sql  # noqa: E402

COB_DT = "2025-12-31"
PREV_COB_DT = "2025-12-30"
OPEN_END = date(9999, 12, 31)
N_CUSTOMERS = 10


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.appName("rfm-direction-regression")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def silver_schemas() -> dict[str, str]:
    """{table: 'col TYPE, col TYPE, ...'} đọc từ DDL Silver thật."""
    text = re.sub(r"--[^\n]*", "", SILVER_DDL.read_text(encoding="utf-8"))
    schemas = {}
    for name, body in re.findall(
        r"CREATE TABLE IF NOT EXISTS lakehouse\.silver\.(\w+)\s*\((.*?)\)\s*USING", text, re.S
    ):
        cols = [" ".join(line.split()) for line in body.split(",\n")]
        schemas[name] = ", ".join(c.strip().rstrip(",") for c in cols if c.strip())
    return schemas


def register(spark, schemas: dict, table: str, rows: list[dict]):
    """Tạo temp view đúng schema DDL; cột không có trong row → NULL."""
    schema = spark.createDataFrame([], schemas[table]).schema
    data = [tuple(r.get(f.name) for f in schema.fields) for r in rows]
    spark.createDataFrame(data, schema).createOrReplaceTempView(table)


def gold_sql(config_name: str, cob_dt: str) -> str:
    path = next(GOLD_DIR.glob(f"*/{config_name}"))
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    sql = render_sql(config["sql"], {"cob_dt": cob_dt})
    return sql.replace("lakehouse.silver.", "").replace("lakehouse.gold.", "")


def run_gold(spark, config_name: str, cob_dt: str = COB_DT) -> dict[int, dict]:
    rows = spark.sql(gold_sql(config_name, cob_dt)).collect()
    by_id = {r["customer_id"]: r.asDict() for r in rows}
    assert len(by_id) == len(rows), f"{config_name}: có customer_id trùng"
    return by_id


def utc(day: int, hour: int = 3) -> datetime:
    return datetime.fromisoformat(f"2025-12-{day:02d}T{hour:02d}:00:00")


@pytest.fixture(scope="module")
def silver(spark):
    """
    10 khách xếp hạng sẵn: khách 1 TỐT NHẤT (gần nhất, nhiều nhất, chi nhiều nhất),
    khách 10 không có giao dịch nào. Mỗi cob_dt là full snapshot của fact.

    Khách 1 có hai version SCD2: RETAIL tới hết 2025-12-30, PRIORITY từ 2025-12-31.
    """
    schemas = silver_schemas()

    customers = []
    for cid in range(1, N_CUSTOMERS + 1):
        base = {
            "customer_id": cid,
            "full_name": f"Nguyen Van {cid}",
            "gender": "Male",
            "date_of_birth": date(1990, 1, 1),
            "branch_code": "HCM01",
            "kyc_status": "VERIFIED",
            "register_date": date(2020, 1, 1),
            "is_active": 1,
        }
        if cid == 1:
            customers.append(
                {
                    **base,
                    "customer_sk": "sk-1-old",
                    "customer_segment": "RETAIL",
                    "effective_from": date(2025, 1, 1),
                    "effective_to": date(2025, 12, 30),
                    "is_current": 0,
                }
            )
            customers.append(
                {
                    **base,
                    "customer_sk": "sk-1-new",
                    "customer_segment": "PRIORITY",
                    "effective_from": date(2025, 12, 31),
                    "effective_to": OPEN_END,
                    "is_current": 1,
                }
            )
        else:
            customers.append(
                {
                    **base,
                    "customer_sk": f"sk-{cid}",
                    "customer_segment": "RETAIL",
                    "effective_from": date(2025, 1, 1),
                    "effective_to": OPEN_END,
                    "is_current": 1,
                }
            )
    register(spark, schemas, "dim_customer", customers)

    accounts = [
        {
            "account_id": 1000 + cid,
            "customer_id": cid,
            "account_type": "CASA",
            "status": "ACTIVE",
            "balance": Decimal("1000000.00"),
            "effective_from": date(2025, 1, 1),
            "effective_to": OPEN_END,
            "is_current": 1,
        }
        for cid in range(1, N_CUSTOMERS + 1)
    ]
    register(spark, schemas, "dim_account", accounts)
    register(spark, schemas, "dim_card", [])
    register(spark, schemas, "dim_loan", [])
    register(spark, schemas, "fact_card_txn", [])
    register(spark, schemas, "fact_crm_interaction", [])
    register(spark, schemas, "fact_online_transaction", [])

    # Khách cid (1..9): (10 - cid) giao dịch, mỗi giao dịch (10 - cid) * 100.
    # Giao dịch cuối của khách cid rơi vào ngày 31 - cid (UTC 03:00 → cùng ngày ICT).
    # Khách 1 có thêm một giao dịch lúc 2025-12-30 20:00 UTC = 2025-12-31 03:00 ICT:
    # recency = 0 theo ngày nghiệp vụ; CAST trần (UTC) sẽ ra 1.
    txns = []
    txn_id = 0
    for cob in (PREV_COB_DT, COB_DT):
        cob_date = date.fromisoformat(cob)
        for cid in range(1, N_CUSTOMERS):
            for i in range(N_CUSTOMERS - cid):
                txn_id += 1
                txns.append(
                    {
                        "txn_id": txn_id,
                        "account_id": 1000 + cid,
                        "customer_id": cid,
                        "txn_date": utc(31 - cid - i if 31 - cid - i >= 1 else 1),
                        "txn_amount": Decimal((N_CUSTOMERS - cid) * 100),
                        "debit_credit": "D",
                        "channel": "MOBILE",
                        "cob_dt": cob_date,
                    }
                )
        txn_id += 1
        txns.append(
            {
                "txn_id": txn_id,
                "account_id": 1001,
                "customer_id": 1,
                "txn_date": datetime.fromisoformat("2025-12-30T20:00:00"),
                "txn_amount": Decimal("900"),
                "debit_credit": "D",
                "channel": "MOBILE",
                "cob_dt": cob_date,
            }
        )
    register(spark, schemas, "fact_txn_account", txns)
    return spark


class TestRfmSegmentDirection:
    def test_best_customer_scores_five_and_is_champion(self, silver):
        row = run_gold(silver, "rfm_segment.yml")[1]
        assert (row["r_score"], row["f_score"], row["m_score"]) == (5, 5, 5)
        assert row["rfm_segment"] == "Champions"

    def test_inactive_customer_scores_one(self, silver):
        row = run_gold(silver, "rfm_segment.yml")[N_CUSTOMERS]
        assert (row["r_score"], row["f_score"], row["m_score"]) == (1, 1, 1)
        assert row["rfm_segment"] not in ("Champions", "Loyal Customers")

    def test_score_is_monotonic_in_customer_quality(self, silver):
        by_id = run_gold(silver, "rfm_segment.yml")
        scores = [by_id[cid]["rfm_score"] for cid in range(1, N_CUSTOMERS + 1)]
        assert scores == sorted(scores, reverse=True), scores

    def test_recency_uses_ict_business_date(self, silver):
        # Giao dịch 2025-12-30 20:00 UTC là ngày 31 theo giờ VN.
        assert run_gold(silver, "rfm_segment.yml")[1]["recency_days"] == 0


class TestCustomer360:
    def test_rfm_scores_match_rfm_segment(self, silver):
        mart = run_gold(silver, "customer_360.yml")
        rfm = run_gold(silver, "rfm_segment.yml")
        for cid in range(1, N_CUSTOMERS + 1):
            got = (mart[cid]["rfm_recency_score"], mart[cid]["rfm_frequency_score"], mart[cid]["rfm_monetary_score"])
            want = (rfm[cid]["r_score"], rfm[cid]["f_score"], rfm[cid]["m_score"])
            assert got == want, f"customer {cid}: 360={got} rfm_segment={want}"
            assert mart[cid]["rfm_segment"] == rfm[cid]["rfm_segment"]

    def test_one_row_per_customer(self, silver):
        assert sorted(run_gold(silver, "customer_360.yml")) == list(range(1, N_CUSTOMERS + 1))

    def test_days_since_last_txn_uses_ict_business_date(self, silver):
        mart = run_gold(silver, "customer_360.yml")
        assert mart[1]["days_since_last_txn"] == 0
        assert mart[1]["churn_flag"] == 0

    def test_customer_without_transactions_is_churn(self, silver):
        assert run_gold(silver, "customer_360.yml")[N_CUSTOMERS]["churn_flag"] == 1


class TestDimensionAsOfCobDt:
    def test_current_cob_dt_uses_new_version(self, silver):
        row = run_gold(silver, "customer_360.yml", COB_DT)[1]
        assert (row["customer_segment"], row["customer_sk"]) == ("PRIORITY", "sk-1-new")

    def test_rerun_of_previous_cob_dt_uses_version_valid_then(self, silver):
        row = run_gold(silver, "customer_360.yml", PREV_COB_DT)[1]
        assert (row["customer_segment"], row["customer_sk"]) == ("RETAIL", "sk-1-old")

    def test_rfm_keeps_one_row_per_customer_on_both_dates(self, silver):
        for cob in (PREV_COB_DT, COB_DT):
            assert sorted(run_gold(silver, "rfm_segment.yml", cob)) == list(range(1, N_CUSTOMERS + 1))
