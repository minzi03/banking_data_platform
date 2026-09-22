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
