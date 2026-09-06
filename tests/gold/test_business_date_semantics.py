"""
Wave 0 — khoá HỢP ĐỒNG business-date TRƯỚC khi đổi timezone.

Hợp đồng:
    storage / instant       = UTC
    business timezone       = Asia/Ho_Chi_Minh
    business_date           = derive TƯỜNG MINH từ instant
    cob_dt                  = orchestration date, ĐỘC LẬP session timezone

Điểm mấu chốt: KHÔNG convert toàn bộ timestamp sang ICT. Timestamp vẫn là
instant UTC; chỉ khi cần ngày/tháng nghiệp vụ mới derive ICT.

Vì sao cần test này TRƯỚC khi migrate: bug cũ là Spark session
`Asia/Ho_Chi_Minh` còn Trino UTC, nên `CAST(ts AS DATE)` cho hai ngày khác
nhau ở 29,2% số dòng. Nếu chỉ đổi Spark sang UTC mà giữ `CAST(ts AS DATE)`,
Gold sẽ ĐỔI NGHĨA ngay (bucket theo UTC thay vì ICT) — nên Wave 1 và Wave 2
phải cùng một cutover, và bộ test này là thứ chứng minh cutover đúng.

Marked `integration`: cần pyspark (CI không cài).
"""

import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark không có trong CI env")

from pyspark.sql import SparkSession

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BUSINESS_TZ = "Asia/Ho_Chi_Minh"

# ─── Fixture biên ngày ───────────────────────────────────────────────────────
# ICT = UTC+7, nên ranh giới ngày nghiệp vụ rơi vào 17:00 UTC.
# (instant UTC, business_date ICT kỳ vọng, mô tả)
BOUNDARY_CASES = [
    ("2026-02-14T16:59:59Z", date(2026, 2, 14), "ngay TRƯỚC ranh giới ICT"),
    ("2026-02-14T17:00:00Z", date(2026, 2, 15), "ĐÚNG ranh giới — sang ngày ICT mới"),
    ("2026-02-14T23:59:59Z", date(2026, 2, 15), "cuối ngày UTC, đã là hôm sau theo ICT"),
    ("2026-02-15T00:00:00Z", date(2026, 2, 15), "đầu ngày UTC, vẫn cùng ngày ICT"),
    ("2026-02-15T16:59:59Z", date(2026, 2, 15), "sát ranh giới kế tiếp"),
    ("2026-02-15T17:00:00Z", date(2026, 2, 16), "ranh giới kế tiếp"),
]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder
        .appName("business-date-semantics")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
        .config("spark.ui.enabled", "false")
        # Baseline SAU migration: session UTC. Test này khoá đúng cấu hình
        # mà Wave 1 sẽ đặt trong spark-defaults.conf.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def events(spark):
    rows = [
        (i, datetime.fromisoformat(utc.replace("Z", "+00:00")), expected)
        for i, (utc, expected, _desc) in enumerate(BOUNDARY_CASES)
    ]
    df = spark.createDataFrame(rows, "id int, event_ts timestamp, expected_business_date date")
    df.createOrReplaceTempView("events")
    return df


class TestBusinessDateDerivation:
    """Biểu thức canonical phải cho đúng ngày nghiệp vụ ICT ở mọi biên."""

    def test_canonical_expression_matches_expected_at_every_boundary(self, spark, events):
        wrong = spark.sql(f"""
            SELECT id,
                   CAST(from_utc_timestamp(event_ts, '{BUSINESS_TZ}') AS DATE) AS actual,
                   expected_business_date AS expected
            FROM events
            WHERE CAST(from_utc_timestamp(event_ts, '{BUSINESS_TZ}') AS DATE)
                  <> expected_business_date
        """).collect()
        assert not wrong, f"lệch ở biên: {[(r['id'], r['actual'], r['expected']) for r in wrong]}"

    def test_naive_cast_is_wrong_at_boundary(self, spark, events):
        """
        Chứng minh vì sao KHÔNG được dùng `CAST(ts AS DATE)` trần.
        Dưới session UTC nó cho ngày UTC, lệch ICT ở mọi instant >= 17:00 UTC.
        Đây chính là 29,2% số dòng đã đo được trên dữ liệu thật.
        """
        mismatches = spark.sql("""
            SELECT COUNT(*) AS n FROM events
            WHERE CAST(event_ts AS DATE) <> expected_business_date
        """).collect()[0]["n"]
        assert mismatches == 3, (
            "3/6 case biên phải lệch khi dùng CAST trần — nếu không, fixture "
            "không còn phủ được ranh giới ngày"
        )

    def test_canonical_expression_requires_utc_session(self, spark, events):
        """
        PHÁT HIỆN QUAN TRỌNG, đo được khi khoá contract:
        `from_utc_timestamp` của Spark KHÔNG độc lập session.

        Spark TIMESTAMP là LTZ (instant). `from_utc_timestamp(ts, tz)` dịch
        INSTANT đi một offset rồi trả về instant khác; `CAST(... AS DATE)` sau
        đó render instant ấy theo SESSION timezone. Đo thực tế:

            session=UTC               → 2026-02-14, 2026-02-15   ĐÚNG
            session=Asia/Ho_Chi_Minh  → 2026-02-15, 2026-02-15   SAI (dịch 2 lần)
            session=America/New_York  → 2026-02-14, 2026-02-14   SAI

        Nên biểu thức canonical chỉ đúng KHI session = UTC. Ta không giả vờ nó
        độc lập session — ta ENFORCE session=UTC trong get_spark_session() và
        spark-defaults.conf, rồi khoá điều kiện đó bằng test.
        """
        expr = f"CAST(from_utc_timestamp(event_ts, '{BUSINESS_TZ}') AS DATE)"
        expected = [c[1] for c in BOUNDARY_CASES]
        original = spark.conf.get("spark.sql.session.timeZone")
        try:
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            under_utc = [r["d"] for r in spark.sql(
                f"SELECT {expr} AS d FROM events ORDER BY id").collect()]
            spark.conf.set("spark.sql.session.timeZone", BUSINESS_TZ)
            under_ict = [r["d"] for r in spark.sql(
                f"SELECT {expr} AS d FROM events ORDER BY id").collect()]
        finally:
            spark.conf.set("spark.sql.session.timeZone", original)

        assert under_utc == expected, "phải đúng dưới session UTC"
        assert under_ict != expected, (
            "nếu biểu thức bỗng đúng ở mọi session thì giả định của kiến trúc "
            "đã đổi — xem lại guard enforce session=UTC"
        )

    def test_canonical_matches_session_independent_oracle(self, spark, events):
        """
        Oracle độc lập session để kiểm chứng biểu thức canonical.

        ICT = UTC+7 cố định (Việt Nam bỏ DST từ 1975) nên có thể tính ngày
        nghiệp vụ thuần bằng số học epoch — hoàn toàn không đụng session tz.
        Oracle này KHÔNG dùng trong production (hard-code offset, sai nếu business
        tz có DST) nhưng là thước đo độc lập chứng minh biểu thức canonical đúng.
        """
        oracle = (
            "date_add(DATE '1970-01-01', "
            "CAST(FLOOR((unix_timestamp(event_ts) + 25200) / 86400) AS INT))"
        )
        canonical = f"CAST(from_utc_timestamp(event_ts, '{BUSINESS_TZ}') AS DATE)"
        original = spark.conf.get("spark.sql.session.timeZone")
        try:
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            rows = spark.sql(
                f"SELECT id, {canonical} AS canon, {oracle} AS oracle, "
                "expected_business_date AS expected FROM events ORDER BY id"
            ).collect()
        finally:
            spark.conf.set("spark.sql.session.timeZone", original)

        for r in rows:
            assert r["canon"] == r["oracle"] == r["expected"], (
                f"id={r['id']}: canon={r['canon']} oracle={r['oracle']} expected={r['expected']}"
            )


class TestSessionTimezoneGuard:
    """
    Vì biểu thức canonical chỉ đúng dưới session=UTC, precondition đó phải được
    ENFORCE chứ không phải giả định. get_spark_session() raise nếu session tz
    khác UTC — biến một giả định ngầm thành lỗi fail-fast.
    """

    def test_spark_defaults_pins_utc(self):
        conf = (PROJECT_ROOT / "docker" / "spark" / "conf" / "spark-defaults.conf").read_text(
            encoding="utf-8"
        )
        line = next(
            ln for ln in conf.splitlines() if ln.strip().startswith("spark.sql.session.timeZone")
        )
        assert line.split()[-1] == "UTC", f"session tz phải là UTC, đang là: {line}"

    def test_spark_session_factory_enforces_utc(self):
        src = (PROJECT_ROOT / "code_etl" / "shared" / "spark" / "spark_session.py").read_text(
            encoding="utf-8"
        )
        # Business tz ĐƯỢC PHÉP xuất hiện như hằng số (BUSINESS_TIMEZONE) —
        # cái không được phép là dùng nó làm SESSION timezone.
        assert 'session.timeZone", "Asia/Ho_Chi_Minh"' not in src, (
            "factory không được set session tz sang business tz — đó chính là bug cũ"
        )
        assert 'session.timeZone", "UTC"' in src, "factory phải pin session tz = UTC"
        assert "assert_utc_session" in src, "phải có guard kiểm session tz"
        assert 'BUSINESS_TIMEZONE = "Asia/Ho_Chi_Minh"' in src, (
            "business tz phải là hằng số tường minh, không rải rác trong code"
        )


class TestInstantRoundTrip:
    """
    Instant phải bảo toàn qua storage. Nếu instant sai thì đó là storage/parsing
    bug, KHÔNG phải business-date bug — hai loại lỗi này cần tách bạch.
    """

    def test_epoch_preserved_regardless_of_session_timezone(self, spark, events):
        original = spark.conf.get("spark.sql.session.timeZone")
        try:
            epochs = {}
            for tz in ("UTC", "Asia/Ho_Chi_Minh"):
                spark.conf.set("spark.sql.session.timeZone", tz)
                epochs[tz] = [
                    r["e"] for r in spark.sql(
                        "SELECT unix_timestamp(event_ts) AS e FROM events ORDER BY id"
                    ).collect()
                ]
        finally:
            spark.conf.set("spark.sql.session.timeZone", original)
        assert epochs["UTC"] == epochs["Asia/Ho_Chi_Minh"], (
            "epoch phải giống nhau ở mọi session tz — instant là bất biến"
        )

    def test_expected_epoch_values(self, spark, events):
        got = [r["e"] for r in spark.sql(
            "SELECT unix_timestamp(event_ts) AS e FROM events ORDER BY id"
        ).collect()]
        want = [
            int(datetime.fromisoformat(c[0].replace("Z", "+00:00"))
                .replace(tzinfo=timezone.utc).timestamp())
            for c in BOUNDARY_CASES
        ]
        assert got == want


class TestCobDtIndependence:
    """
    cob_dt là orchestration date, KHÔNG phải hàm của event timestamp.
    Timezone migration không được làm đổi nghĩa của nó.
    """

    def test_cob_dt_is_not_derived_from_event_timestamp(self, spark, events):
        rows = spark.sql("""
            SELECT DATE '2026-09-06' AS cob_dt,
                   CAST(event_ts AS DATE) AS event_utc_date
            FROM events
        """).collect()
        assert all(r["cob_dt"] == date(2026, 9, 6) for r in rows)
        assert {r["event_utc_date"] for r in rows} != {date(2026, 9, 6)}, (
            "cob_dt phải độc lập với event date"
        )
