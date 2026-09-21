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
