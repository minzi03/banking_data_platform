"""
Executable regression cho hai lỗi P0 của tầng Gold.

Chạy SQL THẬT lấy từ YAML THẬT trên dữ liệu in-memory, nên nếu ai đó viết lại
join sai grain hoặc bỏ điều kiện cob_dt thì test fail với con số cụ thể.

Marked `integration`: CI hiện chỉ cài pyyaml/pydantic/pytest/jinja2, không có
pyspark. Chạy local bằng:

    pytest tests/gold/test_gold_fanout_regression.py -m integration

Kịch bản:
    Case 1 — fan-out account × card   : 10 acct + 5 card, không được thành 50
    Case 2 — snapshot duplication     : cùng N txn ở 2 cob_dt, Gold(D2) = N
    Case 3 — single-channel customer  : chỉ acct hoặc chỉ card vẫn đúng
    Case 4 — fan-out card × card txn  : 2 thẻ × 10 txn, amount không nhân đôi
    Case 5 — cross-model reconciliation: churn.txn_amt_30d = summary.total_txn_amount_30d
    Case 6 — missing physical snapshot: cob_dt không tồn tại → job FAIL, không
             im lặng dùng partition khác và cũng không im lặng ghi số 0
"""

import os
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

pyspark = pytest.importorskip("pyspark", reason="pyspark không có trong CI env")

from pyspark.sql import SparkSession

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "code_etl" / "gold"
sys.path.insert(0, str(PROJECT_ROOT / "code_etl" / "shared"))

from utils.logger import get_logger
from utils.sql_renderer import render_sql

COB_DT = "2025-12-31"
PREV_COB_DT = "2025-12-30"


# ---------------------------------------------------------------------------
# Spark + SQL harness
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    # Executor/driver phải dùng đúng interpreter đang chạy pytest.
    # Trên Windows, `python` trần có thể trúng Microsoft Store alias stub và
    # worker sẽ không connect back được.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder
        .appName("gold-fanout-regression")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def gold_sql(config_name: str, cob_dt: str = COB_DT) -> str:
    """
    Lấy SQL thật từ YAML, render cob_dt, rồi map tên bảng 3 phần
    (lakehouse.silver.x) về temp view 1 phần (x) để chạy in-memory.
    """
    path = next(GOLD_DIR.glob(f"*/{config_name}"))
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    sql = render_sql(config["sql"], {"cob_dt": cob_dt})
    return sql.replace("lakehouse.silver.", "").replace("lakehouse.gold.", "")


def run_gold(spark, config_name: str, cob_dt: str = COB_DT):
    return spark.sql(gold_sql(config_name, cob_dt))


def one(df, customer_id: int):
    rows = df.filter(df.customer_id == customer_id).collect()
    assert len(rows) == 1, f"customer {customer_id}: mong đợi 1 dòng, nhận {len(rows)}"
    return rows[0]


# ---------------------------------------------------------------------------
# Fixtures: dữ liệu Silver in-memory
#
# Mỗi cob_dt chứa FULL SNAPSHOT của fact — đúng như bronze full_snapshot
# hiện tại của platform. Đó chính là điều kiện làm lộ lỗi ②.
# ---------------------------------------------------------------------------

def ts(day: int, hour: int = 10) -> datetime:
    """Naive timestamp — khớp với TIMESTAMP không timezone của Silver fact."""
    return datetime.fromisoformat(f"2025-12-{day:02d}T{hour:02d}:00:00")


@pytest.fixture(scope="module")
def silver_tables(spark):
    # --- dim_customer (SCD2) ------------------------------------------------
    customers = [
        (1, "sk-1", 1),   # cả account lẫn card  → Case 1
        (2, "sk-2", 1),   # chỉ account          → Case 3
        (3, "sk-3", 1),   # chỉ card             → Case 3
        (4, "sk-4", 1),   # không giao dịch      → Case 3
        (9, "sk-9", 0),   # không current → phải bị loại
    ]
    spark.createDataFrame(
        customers, "customer_id long, customer_sk string, is_current int"
    ).createOrReplaceTempView("dim_customer")

    # --- fact_txn_account ---------------------------------------------------
    # customer 1: 10 txn × 100.00 = 1000.00
    # customer 2:  4 txn × 250.00 = 1000.00
    acct_rows = []
    txn_id = 0
    for cob in (PREV_COB_DT, COB_DT):
        cob_date = date.fromisoformat(cob)
        for i in range(10):
            txn_id += 1
            acct_rows.append((txn_id, 1, ts(20 + i % 10), Decimal("100.00"), "D", "MOBILE", cob_date))
        for i in range(4):
            txn_id += 1
            acct_rows.append((txn_id, 2, ts(20 + i), Decimal("250.00"), "C", "ATM", cob_date))
    spark.createDataFrame(
        acct_rows,
        "txn_id long, customer_id long, txn_date timestamp, txn_amount decimal(18,2), "
        "debit_credit string, channel string, cob_dt date",
    ).createOrReplaceTempView("fact_txn_account")

    # --- fact_card_txn ------------------------------------------------------
    # customer 1: 5 txn × 100.00 = 500.00  (trên 2 thẻ → Case 4)
    # customer 3: 2 txn × 300.00 = 600.00
    # + 1 txn FAILED và 1 txn REFUND của customer 1 → phải bị loại khỏi amount
    card_rows = []
    ctxn_id = 0
    for cob in (PREV_COB_DT, COB_DT):
        cob_date = date.fromisoformat(cob)
        for i in range(5):
            ctxn_id += 1
            card_rows.append((ctxn_id, 101 + (i % 2), 1, ts(22 + i % 8), Decimal("100.00"),
                              "PURCHASE", "SUCCESS", "GROCERY", cob_date))
        for i in range(2):
            ctxn_id += 1
            card_rows.append((ctxn_id, 301, 3, ts(25 + i), Decimal("300.00"),
                              "PURCHASE", "SUCCESS", "TRAVEL", cob_date))
        ctxn_id += 1
        card_rows.append((ctxn_id, 101, 1, ts(26), Decimal("999.00"),
                          "PURCHASE", "FAILED", "GROCERY", cob_date))
        ctxn_id += 1
        card_rows.append((ctxn_id, 101, 1, ts(26), Decimal("777.00"),
                          "REFUND", "SUCCESS", "GROCERY", cob_date))
    spark.createDataFrame(
        card_rows,
        "txn_id long, card_id long, customer_id long, txn_date timestamp, "
        "txn_amount decimal(18,2), txn_type string, status string, "
        "merchant_category string, cob_dt date",
    ).createOrReplaceTempView("fact_card_txn")

    # --- dim_card (customer 1 giữ 2 thẻ → nhân đôi nếu join sai grain) ------
    spark.createDataFrame(
        [
            (101, 1, "CREDIT", "ACTIVE", Decimal("50000.00")),
            (102, 1, "DEBIT", "ACTIVE", Decimal("0.00")),
            (301, 3, "CREDIT", "ACTIVE", Decimal("20000.00")),
        ],
        "card_id long, customer_id long, card_type string, status string, "
        "credit_limit decimal(18,2)",
    ).createOrReplaceTempView("dim_card")

    return spark


# ---------------------------------------------------------------------------
# Case 1 — fan-out account × card
# ---------------------------------------------------------------------------

class TestCase1FanOutAccountCard:
    def test_rfm_frequency_and_monetary_not_amplified(self, silver_tables, spark):
        row = one(run_gold(spark, "rfm_segment.yml"), 1)

        # frequency = 10 account + 6 card SUCCESS (5 PURCHASE + 1 REFUND)
        # Không phải 50 dòng joined.
        assert row["frequency"] == 16
        # monetary = 10×100 (account) + 5×100 (card, REFUND bị loại) = 1500.00
        # Nếu fan-out quay lại: account bị ×6 và card bị ×10 → 11000.00
        assert row["monetary"] == Decimal("1500.00")

    def test_churn_counts_and_amounts_not_amplified(self, silver_tables, spark):
        row = one(run_gold(spark, "churn_prediction.yml"), 1)
        assert row["txn_cnt_30d"] == 16
        assert row["txn_cnt_90d"] == 16
        assert row["txn_amt_30d"] == Decimal("1500.00")
        assert row["txn_amt_90d"] == Decimal("1500.00")

    def test_failed_and_refund_card_txn_excluded_from_amount(self, silver_tables, spark):
        """
        Semantics gốc được giữ nguyên (chỉ sửa grain, không sửa business rule):
          - FAILED  → loại khỏi CẢ count lẫn amount (filter status = 'SUCCESS')
          - REFUND  → vẫn tính vào count, chỉ loại khỏi amount (filter txn_type)
        """
        row = one(run_gold(spark, "rfm_segment.yml"), 1)
        assert row["monetary"] == Decimal("1500.00")
        assert row["frequency"] == 16


# ---------------------------------------------------------------------------
# Case 2 — snapshot duplication qua nhiều cob_dt
# ---------------------------------------------------------------------------

class TestCase2SnapshotDuplication:
    def test_two_cob_dt_partitions_exist_in_source(self, silver_tables, spark):
        """Sanity: dữ liệu test thật sự có 2 snapshot, nếu không Case 2 vô nghĩa."""
        cobs = [r[0] for r in spark.sql(
            "SELECT DISTINCT cob_dt FROM fact_txn_account ORDER BY cob_dt"
        ).collect()]
        assert cobs == [date.fromisoformat(PREV_COB_DT), date.fromisoformat(COB_DT)]

    def test_gold_reads_single_snapshot_only(self, silver_tables, spark):
        row = one(run_gold(spark, "customer_transaction_summary.yml"), 1)
        assert row["acct_txn_count_30d"] == 10        # không phải 20
        assert row["acct_txn_amount_30d"] == Decimal("1000.00")
        assert row["card_txn_count_30d"] == 6         # 5 SUCCESS + 1 REFUND SUCCESS
        assert row["total_txn_amount_30d"] == Decimal("1500.00")

    def test_same_result_for_each_cob_dt(self, silver_tables, spark):
        """Cùng population ở D1 và D2 → Gold ra cùng con số, không phải 2N."""
        d2 = one(run_gold(spark, "customer_transaction_summary.yml", COB_DT), 1)
        d1 = one(run_gold(spark, "customer_transaction_summary.yml", PREV_COB_DT), 1)
        assert d1["acct_txn_count_30d"] == d2["acct_txn_count_30d"] == 10
        assert d1["acct_txn_amount_30d"] == d2["acct_txn_amount_30d"]

    def test_rfm_stable_across_snapshots(self, silver_tables, spark):
        d2 = one(run_gold(spark, "rfm_segment.yml", COB_DT), 1)
        d1 = one(run_gold(spark, "rfm_segment.yml", PREV_COB_DT), 1)
        assert d1["frequency"] == d2["frequency"] == 16
        assert d1["monetary"] == d2["monetary"] == Decimal("1500.00")


# ---------------------------------------------------------------------------
# Case 3 — khách chỉ có một kênh giao dịch
# ---------------------------------------------------------------------------

class TestCase3SingleChannelCustomer:
    def test_account_only_customer(self, silver_tables, spark):
        row = one(run_gold(spark, "rfm_segment.yml"), 2)
        assert row["frequency"] == 4
        assert row["monetary"] == Decimal("1000.00")
        assert row["recency_days"] is not None
        assert row["recency_days"] < 365

    def test_card_only_customer(self, silver_tables, spark):
        row = one(run_gold(spark, "rfm_segment.yml"), 3)
        assert row["frequency"] == 2
        assert row["monetary"] == Decimal("600.00")
        assert row["recency_days"] is not None
        assert row["recency_days"] < 365

    def test_customer_with_no_transactions_keeps_sentinel_semantics(self, silver_tables, spark):
        row = one(run_gold(spark, "rfm_segment.yml"), 4)
        assert row["frequency"] == 0
        assert row["monetary"] == Decimal("0.00")
        # sentinel 1900-01-01 → recency rất lớn, KHÔNG phải NULL
        assert row["recency_days"] is not None
        assert row["recency_days"] > 40000

    def test_churn_flags_inactive_customer(self, silver_tables, spark):
        row = one(run_gold(spark, "churn_prediction.yml"), 4)
        assert row["churn_risk"] == "High"
        assert row["is_churn_candidate"] == 1

    def test_non_current_customer_excluded(self, silver_tables, spark):
        ids = [r["customer_id"] for r in run_gold(spark, "rfm_segment.yml").collect()]
        assert 9 not in ids
        assert sorted(ids) == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Case 4 — fan-out dim_card × fact_card_txn
# ---------------------------------------------------------------------------

class TestCase4FanOutCardHolding:
    def test_card_summary_amount_not_multiplied_by_card_count(self, silver_tables, spark):
        row = one(run_gold(spark, "customer_card_summary.yml"), 1)
        assert row["total_cards"] == 2
        assert row["total_card_txn_count_30d"] == 6
        # 5 × 100 = 500. Nếu join thẳng dim_card × fact_card_txn → 2 thẻ × 500 = 1000
        assert row["total_card_txn_amount_30d"] == Decimal("500.00")

    def test_card_holding_counts_still_correct(self, silver_tables, spark):
        row = one(run_gold(spark, "customer_card_summary.yml"), 1)
        assert row["cnt_credit_active"] == 1
        assert row["cnt_debit_active"] == 1
        assert row["max_credit_limit"] == Decimal("50000.00")

    def test_customer_without_cards_gets_zeros(self, silver_tables, spark):
        row = one(run_gold(spark, "customer_card_summary.yml"), 2)
        assert row["total_cards"] == 0
        assert row["total_card_txn_count_30d"] == 0
        assert row["total_card_txn_amount_30d"] == Decimal("0.00")
        assert row["last_card_txn_date"] is None


# ---------------------------------------------------------------------------
# Case 5 — cross-model reconciliation
# ---------------------------------------------------------------------------

class TestCase5CrossModelReconciliation:
    """
    churn.txn_amt_30d và customer_transaction_summary.total_txn_amount_30d dùng
    cùng population và cùng cửa sổ 30 ngày → phải bằng nhau tuyệt đối.
    Đây chính là invariant ngăn bug fan-out quay lại ở một trong hai model.

    RFM.monetary KHÔNG nằm trong test này vì nó dùng cửa sổ 90 ngày —
    semantics khác, không phải bug.
    """

    @staticmethod
    def _metric(spark, config_name: str, column: str) -> dict:
        """
        Chỉ select cột cần so sánh.
        Không collect cột TIMESTAMP: sentinel 1900-01-01 không materialize được
        qua datetime.fromtimestamp() trên Windows (OSError 22) — giới hạn của
        harness, không phải của SQL.
        """
        rows = run_gold(spark, config_name).select("customer_id", column).collect()
        return {r["customer_id"]: r[column] for r in rows}

    def test_churn_amount_reconciles_with_transaction_summary(self, silver_tables, spark):
        churn = self._metric(spark, "churn_prediction.yml", "txn_amt_30d")
        summary = self._metric(
            spark, "customer_transaction_summary.yml", "total_txn_amount_30d"
        )
        assert set(churn) == set(summary)
        for cid in churn:
            assert churn[cid] == summary[cid], (
                f"customer {cid}: churn={churn[cid]} vs summary={summary[cid]}"
            )

    def test_churn_count_reconciles_with_transaction_summary(self, silver_tables, spark):
        churn = self._metric(spark, "churn_prediction.yml", "txn_cnt_30d")
        summary = self._metric(
            spark, "customer_transaction_summary.yml", "total_txn_count_30d"
        )
        assert set(churn) == set(summary)
        for cid in churn:
            assert churn[cid] == summary[cid], (
                f"customer {cid}: churn={churn[cid]} vs summary={summary[cid]}"
            )


# ---------------------------------------------------------------------------
# Case 6 — missing physical snapshot phải FAIL LOUD
# ---------------------------------------------------------------------------

def _load_gold_job_module():
    """Import gold_job.py theo đường dẫn (nó tự set sys.path khi import)."""
    import importlib.util

    path = PROJECT_ROOT / "code_etl" / "gold" / "base_job" / "gold_job.py"
    spec = importlib.util.spec_from_file_location("gold_job_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestCase6MissingSnapshotFailsLoud:
    """
    Nguồn chỉ có D1 (2025-12-30) và D2 (2025-12-31).
    Yêu cầu chạy D3 (2025-12-29) — partition không tồn tại.

    Kỳ vọng: job FAIL.
    KHÔNG được: âm thầm dùng D2.
    KHÔNG được: âm thầm ghi kết quả rỗng.
    KHÔNG được: âm thầm ghi metric toàn 0 rồi báo SUCCESS.
    """

    MISSING_COB_DT = "2025-12-29"

    @staticmethod
    def _config(require_snapshots=None, require_non_empty=True):
        return {
            "target": {"catalog": "lakehouse", "schema": "gold", "table": "rfm_segment"},
            "validation": {
                "require_non_empty": require_non_empty,
                # tên 1 phần → trỏ thẳng temp view trong test
                "require_snapshots": require_snapshots or ["fact_txn_account", "fact_card_txn"],
            },
        }

    def test_missing_snapshot_raises(self, silver_tables, spark):
        gold_job = _load_gold_job_module()
        logger = get_logger("case6")
        with pytest.raises(RuntimeError, match="Thiếu snapshot nguồn"):
            gold_job.assert_source_snapshots(
                spark, self._config(), self.MISSING_COB_DT, logger
            )

    def test_present_snapshot_passes(self, silver_tables, spark):
        gold_job = _load_gold_job_module()
        logger = get_logger("case6")
        gold_job.assert_source_snapshots(spark, self._config(), COB_DT, logger)

    def test_error_message_names_the_missing_tables(self, silver_tables, spark):
        gold_job = _load_gold_job_module()
        logger = get_logger("case6")
        with pytest.raises(RuntimeError) as exc:
            gold_job.assert_source_snapshots(
                spark, self._config(), self.MISSING_COB_DT, logger
            )
        message = str(exc.value)
        assert "fact_txn_account" in message
        assert "fact_card_txn" in message
        assert self.MISSING_COB_DT in message

    def test_missing_snapshot_would_NOT_be_caught_by_non_empty_alone(
        self, silver_tables, spark
    ):
        """
        Đây là lý do tồn tại của require_snapshots.

        rfm_segment neo vào dim_customer rồi LEFT JOIN fact. Khi partition fact
        của cob_dt không tồn tại, query vẫn trả đủ 1 dòng/khách với metric = 0.
        Output KHÔNG rỗng → require_non_empty PASS → Gold bị ghi đè bằng số 0
        trông hoàn toàn hợp lý. Silent corruption.
        """
        gold_job = _load_gold_job_module()
        logger = get_logger("case6")

        df = run_gold(spark, "rfm_segment.yml", self.MISSING_COB_DT)

        assert df.count() > 0, "kỳ vọng output KHÔNG rỗng — đó chính là vấn đề"
        assert df.filter("frequency > 0").count() == 0, "mọi metric phải bằng 0"

        # require_non_empty một mình: PASS (không raise) → không đủ
        gold_job.assert_non_empty(df, self._config(), self.MISSING_COB_DT, logger)

        # require_snapshots: FAIL đúng như mong đợi
        with pytest.raises(RuntimeError, match="Thiếu snapshot nguồn"):
            gold_job.assert_source_snapshots(
                spark, self._config(), self.MISSING_COB_DT, logger
            )

    def test_non_empty_guard_catches_fact_anchored_model(self, silver_tables, spark):
        """
        Guard phụ vẫn cần: model neo fact (branch_monthly_summary) khi thiếu
        snapshot sẽ ra RỖNG, và overwritePartitions() với DataFrame rỗng là
        no-op — partition cũ ở lại mà không ai biết.
        """
        gold_job = _load_gold_job_module()
        logger = get_logger("case6")

        empty_df = spark.sql("SELECT * FROM fact_txn_account WHERE 1 = 0")
        with pytest.raises(RuntimeError, match="không sinh dòng nào"):
            gold_job.assert_non_empty(
                empty_df, self._config(), self.MISSING_COB_DT, logger
            )

    def test_guards_are_opt_in_via_config(self, silver_tables, spark):
        """Không khai báo validation → guard im lặng, không phá model khác."""
        gold_job = _load_gold_job_module()
        logger = get_logger("case6")
        empty_df = spark.sql("SELECT * FROM fact_txn_account WHERE 1 = 0")

        no_validation = {"target": {"catalog": "lakehouse", "schema": "gold", "table": "x"}}
        gold_job.assert_source_snapshots(spark, no_validation, self.MISSING_COB_DT, logger)
        gold_job.assert_non_empty(empty_df, no_validation, self.MISSING_COB_DT, logger)
