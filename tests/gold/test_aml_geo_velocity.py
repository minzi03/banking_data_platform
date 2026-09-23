"""
Regression — geo_velocity_flag trong aml_monitoring
====================================================

Typology geo-velocity: khách hàng giao dịch ở nhiều tỉnh/thành khác nhau trong
cùng một ngày nghiệp vụ — chỉ báo chuẩn của account/card takeover.

Hai điểm khiến nó dễ hỏng âm thầm, và đó là lý do có file này:

1. **Chỉ kênh online mang `location_id`.** `fact_txn_account` không có cột địa
   lý nào. Ai đó "đơn giản hoá" bằng cách bỏ join sang
   `fact_online_transaction` sẽ làm flag im lặng về 0 cho mọi khách hàng.

2. **`require_snapshots` phải chặn.** geo_agg join bằng LEFT JOIN, nên partition
   `fact_online_transaction` thiếu sẽ cho `distinct_states = 0` → flag = 0 →
   bảng đầy đủ dòng, không check nào đỏ, alert_score tụt trên diện rộng. Đúng
   kịch bản silent corruption của ADR-0005.

Ngưỡng >= 3 được ĐO, không chọn cảm tính. Phân phối số tỉnh/thành riêng biệt
mỗi customer-day trên snapshot 2026-09-20:

    1 state  412.786  95,0%
    2 states  21.205   4,9%
    3 states     673   0,2%
    4 states      15   0,0%

Đối chứng với nhãn `is_fraud` cùng snapshot: nhóm gắn cờ 2,0% fraud so với
0,8% ở nhóm không gắn cờ — lift 2,5×.

Chạy: pytest tests/gold/test_aml_geo_velocity.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
AML_CONFIG = REPO_ROOT / "code_etl" / "gold" / "risk" / "aml_monitoring.yml"
GOLD_DDL = REPO_ROOT / "docker" / "init_iceberg" / "03_ddl_gold.sql"

ALL_FLAGS = (
    "high_value_flag",
    "structuring_flag",
    "velocity_flag",
    "multi_channel_flag",
    "geo_velocity_flag",
)


BUSINESS_DAY_OF_ONLINE_TXN = "from_utc_timestamp(transaction_date, 'Asia/Ho_Chi_Minh')"


def _strip_comments(sql: str) -> str:
    """
    Bỏ comment `--`. Comment trong các job này nhắc tên cột (vd. "kiểm chứng
    trên nhãn is_fraud") và chứa ngoặc chưa cân — để lại thì cả việc tách CTE
    lẫn việc tìm tên cột đều đọc nhầm văn xuôi thành code.
    """
    return re.sub(r"--[^\n]*", "", sql)


def _cte_body(sql: str, name: str) -> str:
    """
    Thân của CTE `name AS (...)`, tách theo ngoặc cân bằng.

    Không tách theo vị trí CTE kế tiếp: thứ tự CTE đổi được, và trong
    aml_monitoring `fraud_agg` đứng SAU `flagged` — tách theo "flagged AS ("
    sẽ lấy cả phần SQL còn lại và mọi assert trên đó đều xanh vô nghĩa.
    """
    sql = _strip_comments(sql)
    marker = f"{name} AS ("
    assert marker in sql, f"Thiếu CTE {name}"
    start = sql.index(marker) + len(marker)
    depth = 1
    for i in range(start, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[start:i]
    raise AssertionError(f"CTE {name} không đóng ngoặc")


def _final_select_list(sql: str) -> str:
    """Danh sách cột của SELECT cuối cùng (phần trước `FROM flagged`)."""
    return _strip_comments(sql).rsplit("FROM flagged", 1)[0].rsplit("SELECT", 1)[-1]


@pytest.fixture(scope="module")
def config() -> dict:
    return yaml.safe_load(AML_CONFIG.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def sql(config: dict) -> str:
    return config["sql"]


class TestGeoSources:
    """Nguồn địa lý phải được khai báo VÀ thực sự dùng."""

    @pytest.mark.parametrize("table", ["silver.fact_online_transaction", "silver.dim_location"])
    def test_geo_source_is_declared(self, config: dict, table: str):
        assert table in config["source"]["tables"], f"{table} không có trong source.tables — lineage sẽ thiếu cạnh này"

    @pytest.mark.parametrize("table", ["fact_online_transaction", "dim_location"])
    def test_geo_source_is_actually_joined(self, sql: str, table: str):
        assert table in sql, (
            f"{table} được khai báo nhưng SQL không đọc. Nếu ai đó bỏ join này, "
            "geo_velocity_flag sẽ im lặng về 0 cho mọi khách hàng."
        )

    def test_online_transaction_blocks_the_write(self, config: dict):
        """
        Guard chặn, không phải guard cảnh báo.

        LEFT JOIN nghĩa là partition thiếu KHÔNG làm output rỗng — nó chỉ làm
        mọi flag về 0. require_non_empty không bắt được trường hợp này; chỉ
        assert_source_snapshots mới bắt được.
        """
        required = config["validation"]["require_snapshots"]
        assert "silver.fact_online_transaction" in required, (
            "fact_online_transaction phải nằm trong require_snapshots. "
            "Thiếu nó, một partition vắng mặt sẽ cho geo_velocity_flag = 0 trên "
            "toàn bộ bảng mà không có gì đỏ."
        )


class TestGeoVelocityLogic:
    def test_flag_exists_in_output(self, sql: str):
        assert "geo_velocity_flag" in sql

    def test_threshold_is_three_states(self, sql: str):
        """Ngưỡng đo được. Đổi nó thì phải đo lại phân phối, không đoán."""
        assert "distinct_states, 0) >= 3" in sql, (
            "Ngưỡng geo-velocity không còn là >= 3 tỉnh/thành. "
            "Nếu cố ý đổi, đo lại phân phối và cập nhật cả docstring của test này."
        )

    def test_counts_distinct_states_not_rows(self, sql: str):
        """
        Đếm DISTINCT state, không phải số giao dịch.

        COUNT(*) sẽ biến nó thành một bản sao của velocity_flag — mất hẳn
        chiều địa lý vốn là toàn bộ ý nghĩa của typology này.
        """
        assert "COUNT(DISTINCT l.state)" in sql

    def test_only_successful_transactions_count(self, sql: str):
        """
        Giao dịch FAILED/PENDING không chứng minh sự hiện diện địa lý.

        Tính cả chúng sẽ thổi phồng distinct_states bằng những lần thử không
        thành công.
        """
        assert "ot.status = 'SUCCESS'" in sql

    def test_geo_aggregation_is_customer_day_grain(self, sql: str):
        """Cùng grain với velocity_flag: customer × ngày nghiệp vụ."""
        assert "GROUP BY ot.customer_id" in sql

    def test_business_date_uses_canonical_expression(self, sql: str):
        """
        Ngày nghiệp vụ phải suy tường minh từ UTC (ADR-0004).

        `CAST(ts AS DATE)` trần sẽ cho ngày khác nhau tuỳ session timezone —
        và ở đây nó còn làm join geo_agg lệch sang ngày khác.
        """
        assert "CAST(from_utc_timestamp(ot.transaction_date, 'Asia/Ho_Chi_Minh') AS DATE)" in sql


class TestScoring:
    def test_geo_velocity_contributes_to_alert_score(self, sql: str):
        assert "geo_velocity_flag*3" in sql, "geo_velocity_flag phải có trọng số trong alert_score"

    def test_structuring_outweighs_geo_velocity(self, sql: str):
        """
        structuring giữ trọng số cao nhất (4).

        Nó là hành vi CỐ Ý né ngưỡng báo cáo, khác với bất thường có thể do
        nguyên nhân hợp pháp. Thứ tự này là chủ ý, không phải tuỳ tiện.
        """
        assert "structuring_flag*4" in sql

    @pytest.mark.parametrize("flag", ALL_FLAGS)
    def test_every_flag_counts_toward_risk_level(self, sql: str, flag: str):
        """
        risk_level và alert_generated phải đếm ĐỦ cả 5 flag.

        Thêm flag mới mà quên cập nhật hai biểu thức này là lỗi im lặng điển
        hình: flag chạy đúng, nhưng không bao giờ ảnh hưởng tới cảnh báo.
        """
        risk_expr = sql.split("AS risk_level")[0].rsplit("CASE WHEN", 1)[-1]
        assert flag in risk_expr, f"{flag} không được tính vào risk_level"

    def test_alert_generated_counts_all_flags(self, sql: str):
        alert_expr = sql.split("AS alert_generated")[0].rsplit("CASE WHEN", 1)[-1]
        for flag in ALL_FLAGS:
            assert flag in alert_expr, f"{flag} không được tính vào alert_generated"


class TestTypologyGrain:
    """
    Mọi typology AML nói về hành vi trong MỘT ngày nghiệp vụ.

    Một partition `cob_dt` KHÔNG phải một ngày giao dịch: `cob_dt` là ngày NẠP.
    Đo được trên snapshot 2026-09-20, partition đó chứa giao dịch trải **432
    ngày nghiệp vụ** (2025-05-30 → 2026-08-04).

    Đây từng là lỗi thật: `multi_channel_flag` tính `COUNT(DISTINCT channel)` ở
    grain partition, nên nó đếm kênh trên ~14 tháng. 94,9% khách hàng chạm đủ
    5 kênh trong khoảng đó → `>= 4` gắn cờ **99,9%** giao dịch. Cộng với
    `high_value_flag` 60%, `alert_generated` (>= 2 flag) bắn trên **60%** toàn
    bảng — model thực chất cảnh báo mọi giao dịch lớn.

    Sửa grain đưa tỷ lệ cảnh báo từ 60% xuống 0,9%.
    """

    def test_channel_count_is_computed_per_day(self, sql: str):
        assert "daily_channel_count" in sql, (
            "channel_count phải tính trong daily_agg (grain customer-day). "
            "Ở grain partition nó đếm kênh trên ~432 ngày và gắn cờ 99,9%."
        )

    def test_customer_stats_does_not_compute_channel_count(self, sql: str):
        """
        `customer_stats` gom toàn partition — chỉ hợp cho tổng credit/debit.

        Nếu `COUNT(DISTINCT channel)` quay lại đây, flag sẽ âm thầm trở lại
        99,9% mà không test nào khác đỏ.
        """
        cs_block = sql.split("customer_stats AS (")[1].split("),")[0]
        assert "COUNT(DISTINCT channel)" not in cs_block, (
            "COUNT(DISTINCT channel) nằm trong customer_stats (grain partition). Nó thuộc về daily_agg."
        )

    def test_multi_channel_reads_the_daily_column(self, sql: str):
        assert "da.daily_channel_count >= 4" in sql


class TestCalibratedThresholds:
    """
    Ngưỡng ĐO trên snapshot 2026-09-20, grain customer-day.

    Đổi bất kỳ số nào ở đây thì phải đo lại phân phối và cập nhật cả docstring.
    Một ngưỡng chọn cảm tính sẽ hoặc không bao giờ chạy, hoặc gắn cờ mọi thứ —
    cả hai đều xảy ra thật trong repo này.
    """

    def test_velocity_threshold_is_reachable(self, sql: str):
        """
        `>= 10` là BẤT KHẢ THI: số giao dịch/khách/ngày tối đa đo được là 8.

        Rule đó chưa từng gắn cờ một dòng nào. Phân phối thật:
            >= 3 txn  2,7%  ·  >= 4 txn  0,4%  ·  >= 5 txn  0,0%
        """
        assert "da.daily_txn_count >= 4" in sql, (
            "Ngưỡng velocity không còn là >= 4. Lưu ý >= 10 không bao giờ chạy "
            "(max thực tế 8/ngày) — nếu đổi, đo lại trước."
        )

    def test_structuring_threshold_fires(self, sql: str):
        """`>= 3` gắn cờ 0,0%; `>= 2` gắn cờ 0,2% ở grain ngày."""
        assert "da.structured_count >= 2" in sql

    def test_high_value_threshold_documents_why_it_is_uncalibrated(self, sql: str):
        """
        `high_value_flag` CỐ Ý giữ 200tr dù nó gắn cờ 60%.

        Gốc rễ không phải ngưỡng mà là generator: `random.uniform(10.000,
        500.000.000)` cho median 250tr — nằm TRÊN ngưỡng. Nâng lên p99 (~495tr)
        sẽ cho tỷ lệ đẹp nhưng vô nghĩa với ngân hàng thật.

        Test này không kiểm con số; nó kiểm rằng lý do vẫn được ghi lại, để
        không ai tưởng 200tr là kết quả của một phép đo.
        """
        assert "ROADMAP 3.6" in sql, (
            "Mất phần giải thích vì sao high_value_flag chưa hiệu chỉnh. "
            "Nếu đã sửa generator, đo lại rồi cập nhật cả comment lẫn test này."
        )


class TestDDLAlignment:
    @pytest.mark.parametrize("column", ["geo_velocity_flag", "distinct_states", "high_risk_locations"])
    def test_column_exists_in_gold_ddl(self, column: str):
        """
        DDL phải có cột trước khi job ghi.

        Iceberg writeTo khớp theo tên; cột thiếu ở đích làm job fail lúc chạy
        chứ không phải lúc review.
        """
        ddl = GOLD_DDL.read_text(encoding="utf-8")
        aml_block = ddl.split("CREATE TABLE IF NOT EXISTS lakehouse.gold.aml_monitoring")[1]
        aml_block = aml_block.split(";")[0]
        assert column in aml_block, f"{column} thiếu trong DDL của gold.aml_monitoring"

    @pytest.mark.parametrize("column", ["geo_velocity_flag", "distinct_states", "high_risk_locations"])
    def test_column_is_selected_by_the_job(self, sql: str, column: str):
        final_select = sql.rsplit("FROM flagged", 1)[0].rsplit("SELECT", 1)[-1]
        assert column in final_select, f"{column} có trong DDL nhưng job không SELECT ra"


class TestGroundTruthFraudColumn:
    """
    `is_fraud` là GROUND TRUTH copy từ fact_online_transaction, không phải feature.

    Mục đích: đo precision/recall của các rule-based flag. Vì thế nó có ba
    ràng buộc dễ vi phạm âm thầm:

    1. Phải lấy từ fact_online_transaction — fact_txn_account không có cột này.
    2. Phải là LEFT JOIN — INNER JOIN sẽ làm rơi mọi giao dịch không fraud,
       biến bảng thành chỉ còn các ca dương tính.
    3. Phải COALESCE về 0 — khách không giao dịch online trong ngày là
       không-fraud, không phải NULL.
    """

    def test_fraud_source_is_declared(self, config: dict):
        assert "silver.fact_online_transaction" in config["source"]["tables"]

    def test_missing_online_partition_blocks_the_job(self, config: dict):
        """
        Partition fact_online_transaction vắng mặt → LEFT JOIN cho is_fraud = 0
        toàn bảng, bảng vẫn đủ dòng. require_non_empty không bắt được; chỉ guard
        snapshot bắt được (ADR-0005).
        """
        assert "silver.fact_online_transaction" in config["validation"]["require_snapshots"]

    def test_fraud_aggregation_is_customer_day_grain(self, sql: str):
        body = _cte_body(sql, "fraud_agg")
        assert "MAX(is_fraud)" in body, "Grain customer-day phải aggregate bằng MAX(is_fraud)"
        group_by = body.split("GROUP BY", 1)[1]
        assert "customer_id" in group_by
        assert BUSINESS_DAY_OF_ONLINE_TXN in group_by, "fraud_agg phải group theo NGÀY nghiệp vụ, không chỉ khách"

    def test_fraud_join_is_left_not_inner(self, sql: str):
        assert "LEFT JOIN fraud_agg" in sql, (
            "INNER JOIN fraud_agg sẽ loại mọi dòng không fraud — bảng chỉ còn "
            "ca dương tính và mọi con số precision đo được đều vô nghĩa."
        )

    def test_fraud_defaults_to_zero_not_null(self, sql: str):
        assert "COALESCE(fa.is_fraud_flag, 0) AS is_fraud" in sql, (
            "Khách không giao dịch online trong ngày là KHÔNG fraud, không phải NULL. "
            "NULL sẽ bị loại khỏi mọi phép đếm precision/recall."
        )

    def test_fraud_join_aligns_on_business_day(self, sql: str):
        """
        Join theo customer_id là chưa đủ — phải kèm ngày nghiệp vụ.

        Thiếu txn_day, nhãn fraud của MỘT ngày sẽ lan sang mọi ngày khác của
        cùng khách hàng, làm tỷ lệ fraud phồng lên theo số ngày hoạt động.
        """
        join_block = sql.split("LEFT JOIN fraud_agg")[1]
        assert "txn_day" in join_block, "Join fraud_agg thiếu điều kiện ngày nghiệp vụ"

    @pytest.mark.parametrize("column", ["is_fraud", "fraud_reason"])
    def test_column_exists_in_gold_ddl(self, column: str):
        ddl = GOLD_DDL.read_text(encoding="utf-8")
        aml_block = ddl.split("CREATE TABLE IF NOT EXISTS lakehouse.gold.aml_monitoring")[1]
        aml_block = aml_block.split(";")[0]
        assert column in aml_block, f"{column} thiếu trong DDL của gold.aml_monitoring"

    def test_fraud_is_not_used_as_a_scoring_input(self, sql: str):
        """
        Ground truth không được lọt vào công thức tính điểm.

        Nếu is_fraud xuất hiện trong alert_score, bảng trở thành tự thoả mãn
        và mọi phép đo precision/recall sau đó đều là vòng tròn.
        """
        flags = _cte_body(sql, "flagged")
        assert "fraud_agg" not in flags and "is_fraud" not in flags, (
            "Ground truth lọt vào CTE tính flag — flag sẽ tự khớp với nhãn."
        )

        # Mọi cột tính điểm (alert_score, risk_level, alert_generated) đứng
        # TRƯỚC cột ground truth trong SELECT cuối.
        scoring = _final_select_list(sql).split("COALESCE(fa.is_fraud_flag", 1)[0]
        assert "alert_score" in scoring and "alert_generated" in scoring
        assert "is_fraud" not in scoring and "fa." not in scoring, (
            "is_fraud lọt vào công thức điểm — ground truth không được là feature. "
            "Mọi phép đo precision/recall sau đó sẽ là vòng tròn."
        )


class TestGroundTruthInFraudRiskTxn:
    """Cùng cột ground truth, nhưng ở gold.fraud_risk_txn."""

    @staticmethod
    def _sql() -> str:
        return yaml.safe_load(
            (REPO_ROOT / "code_etl" / "gold" / "risk" / "fraud_risk_txn.yml").read_text(encoding="utf-8")
        )["sql"]

    @staticmethod
    def _cfg() -> dict:
        return yaml.safe_load(
            (REPO_ROOT / "code_etl" / "gold" / "risk" / "fraud_risk_txn.yml").read_text(encoding="utf-8")
        )

    def test_fraud_source_is_declared(self):
        cfg = self._cfg()
        assert "silver.fact_online_transaction" in cfg["source"]["tables"]
        assert "silver.fact_online_transaction" in cfg["upstream_flags"]

    def test_missing_online_partition_blocks_the_job(self):
        """Cùng lý do như ở aml_monitoring: thiếu partition → is_fraud = 0 im lặng."""
        assert "silver.fact_online_transaction" in self._cfg()["validation"]["require_snapshots"]

    def test_left_join_and_coalesce(self):
        sql = self._sql()
        assert "LEFT JOIN fraud_agg" in sql
        assert "COALESCE(fa.is_fraud_flag, 0) AS is_fraud" in sql

    def test_fraud_aggregation_is_customer_day_grain_and_join_uses_it(self):
        sql = self._sql()
        body = _cte_body(sql, "fraud_agg")
        assert "MAX(is_fraud)" in body
        assert BUSINESS_DAY_OF_ONLINE_TXN in body.split("GROUP BY", 1)[1]
        assert "txn_day" in sql.split("LEFT JOIN fraud_agg", 1)[1], "Join thiếu điều kiện ngày nghiệp vụ"

    def test_fraud_is_not_used_as_a_scoring_input(self):
        sql = self._sql()
        flags = _cte_body(sql, "flagged")
        assert "fraud_agg" not in flags and "is_fraud" not in flags
        scoring = _final_select_list(sql).split("COALESCE(fa.is_fraud_flag", 1)[0]
        assert "risk_score" in scoring and "is_fraud" not in scoring and "fa." not in scoring

    def test_column_exists_in_gold_ddl(self):
        ddl = GOLD_DDL.read_text(encoding="utf-8")
        block = ddl.split("CREATE TABLE IF NOT EXISTS lakehouse.gold.fraud_risk_txn")[1].split(";")[0]
        assert "is_fraud" in block
