"""
Static invariants cho toàn bộ Gold SQL — chạy được trong CI, không cần Spark.

Hai lỗi P0 đã xảy ra và không được phép quay lại:

  ① Fan-out: join >1 raw fact trong cùng một aggregation grain → Cartesian
     amplification, SUM() bị nhân theo số dòng của fact còn lại.

  ② Multi-cob_dt double count: Silver fact FULL SNAPSHOT mỗi cob_dt.
     Query Gold chỉ filter txn_date (business time) mà không chốt cob_dt
     (physical snapshot) sẽ cộng chồng mọi partition đang tồn tại.

  Từ ADR-0018 (2026-09-30) ba fact giao dịch nạp TĂNG DẦN: partition cob_dt là
  ngày nghiệp vụ của giao dịch, mỗi giao dịch nằm đúng một partition. Với chúng
  lỗi ngược lại mới nguy hiểm: pin `cob_dt = cob` chỉ đọc MỘT ngày giao dịch. Rule A
  vì vậy tách hai loại nguồn (INCREMENTAL_FACTS lấy từ job.incremental của Silver).

Test này parse SQL thật trong YAML thật và enforce theo từng scope
(mỗi CTE là một scope, phần còn lại là scope `__main__`).

═══════════════════════════════════════════════════════════════════════════
HAI RULE ĐỘC LẬP — ĐỌC KỸ TRƯỚC KHI "SIẾT CHẶT" TEST NÀY
═══════════════════════════════════════════════════════════════════════════

Rule A — UNIVERSAL, áp dụng cho MỌI model:
    Mọi snapshot-backed source tham gia một Gold build theo cob_dt đều phải
    được pin về đúng snapshot đang xử lý, trừ khi query CỐ Ý muốn đọc
    cross-snapshot history. Áp dụng cho cả Silver→Gold lẫn Gold→Gold.
    Fact incremental: KHÔNG pin một ngày; phải có cận trên `<= cob_dt` (khoảng
    BETWEEN DATE_ADD(cob, -N) AND cob, hoặc `<= cob` khi đọc toàn lịch sử), và cận
    dưới phải phủ hết cửa sổ nghiệp vụ trong cùng scope.

Rule B — MODEL-SPECIFIC, KHÔNG áp dụng cho mọi model:
    Business-time filtering (rolling window 30/90 ngày trên txn_date) là
    quyết định của từng model.
      - Model rolling (rfm, churn, các *_summary, customer_360) BẮT BUỘC có.
      - Model calendar (branch_monthly_summary) CỐ Ý KHÔNG có: nó gom cả
        lịch sử theo tháng dương lịch bên trong MỘT physical snapshot.

⚠ Đừng biến Rule B thành universal. Thêm branch_monthly_summary vào danh sách
  bắt buộc-có-rolling-window sẽ làm hỏng đúng model đó. Assertion đầu tiên khi
  viết test này đã mắc lỗi ấy và phải sửa lại.
"""

import re
from pathlib import Path
from typing import ClassVar

import pytest
import yaml

GOLD_DIR = Path(__file__).resolve().parents[2] / "code_etl" / "gold"

# `cob_dt = DATE '{{ cob_dt }}'`, cho phép prefix alias (t.cob_dt, r.cob_dt)
COB_DT_PIN = re.compile(
    r"\b(?:\w+\.)?cob_dt\s*=\s*DATE\s*'\{\{\s*cob_dt\s*\}\}'",
    re.IGNORECASE,
)
SILVER_FACT_REF = re.compile(r"lakehouse\.silver\.(fact_\w+)", re.IGNORECASE)
# Fact incremental (ADR-0018): khoảng partition có cận trên = cob_dt.
INCREMENTAL_RANGE = re.compile(
    r"\b(?:\w+\.)?cob_dt\s*(?:<=\s*DATE\s*'\{\{\s*cob_dt\s*\}\}'"
    r"|BETWEEN\s+DATE_ADD\(DATE\s*'\{\{\s*cob_dt\s*\}\}',\s*-(\d+)\)\s+AND\s+DATE\s*'\{\{\s*cob_dt\s*\}\}')",
    re.IGNORECASE,
)
BUSINESS_WINDOW = re.compile(r"DATE_ADD\(DATE\s*'\{\{\s*cob_dt\s*\}\}',\s*-(\d+)\)", re.IGNORECASE)
SILVER_FACTS_DIR = Path(__file__).resolve().parents[2] / "code_etl" / "silver" / "facts"
GOLD_REF = re.compile(r"lakehouse\.gold\.(\w+)", re.IGNORECASE)
CTE_HEAD = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\($", re.IGNORECASE)


def incremental_facts() -> set[str]:
    """Tên bảng Silver fact có job.incremental: true (ADR-0018)."""
    out = set()
    for path in SILVER_FACTS_DIR.glob("*.yml"):
        config = yaml.safe_load(path.read_text(encoding="utf-8"))
        if (config.get("job") or {}).get("incremental"):
            out.add(config["target"]["table"])
    return out


INCREMENTAL_FACTS = incremental_facts()


def pinned_or_bounded(sql: str) -> bool:
    return bool(COB_DT_PIN.search(sql) or INCREMENTAL_RANGE.search(sql))


def gold_config_paths() -> list[Path]:
    return sorted(GOLD_DIR.glob("*/*.yml"))


def load_sql(path: Path) -> str:
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    return config.get("sql", "")


def split_sql_scopes(sql: str) -> dict[str, str]:
    """
    Tách SQL thành các scope: mỗi CTE `name AS ( ... )` là một scope riêng,
    phần còn lại (query ngoài cùng) là `__main__`.

    Fan-out và thiếu cob_dt đều là lỗi *trong phạm vi một scope*, nên phải
    kiểm tra theo scope chứ không phải trên cả chuỗi SQL.
    """
    scopes: dict[str, str] = {}
    rest: list[str] = []
    depth = 0
    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]
        if ch == "(":
            head = "".join(rest)[-120:] + "("
            match = CTE_HEAD.search(head)
            if depth == 0 and match:
                inner_depth = 1
                j = i + 1
                while j < n and inner_depth > 0:
                    if sql[j] == "(":
                        inner_depth += 1
                    elif sql[j] == ")":
                        inner_depth -= 1
                    j += 1
                scopes[match.group(1)] = sql[i + 1 : j - 1]
                rest.append(" ")
                i = j
                continue
            depth += 1
        elif ch == ")":
            depth -= 1
        rest.append(ch)
        i += 1

    scopes["__main__"] = "".join(rest)
    return scopes


def iter_scopes_with(pattern: re.Pattern):
    """Yield (config_path, scope_name, scope_sql, matched_names) cho mọi Gold SQL."""
    for path in gold_config_paths():
        for scope_name, scope_sql in split_sql_scopes(load_sql(path)).items():
            matched = set(pattern.findall(scope_sql))
            if matched:
                yield path, scope_name, scope_sql, matched


class TestNoFactFanOut:
    """① Không được join nhiều raw fact trong cùng một aggregation grain."""

    def test_at_most_one_silver_fact_per_scope(self):
        violations = [
            f"{path.name} :: scope `{scope}` reads {sorted(facts)}"
            for path, scope, _sql, facts in iter_scopes_with(SILVER_FACT_REF)
            if len(facts) > 1
        ]
        assert not violations, (
            "Fan-out risk — mỗi fact phải tự aggregate về grain customer_id "
            "trong CTE riêng rồi mới join:\n  " + "\n  ".join(violations)
        )

    def test_gold_configs_are_discovered(self):
        # Guard: nếu glob hỏng thì các test trên sẽ pass rỗng một cách vô nghĩa.
        assert len(gold_config_paths()) >= 10


class TestRuleA_SnapshotPinningIsUniversal:
    """
    Rule A (universal): mọi snapshot-backed source phải pin về cob_dt đang xử lý.
    Áp dụng cho MỌI Gold model, cả Silver→Gold lẫn Gold→Gold.
    """

    def test_incremental_facts_are_discovered(self):
        assert {"fact_txn_account", "fact_card_txn", "fact_online_transaction"} <= INCREMENTAL_FACTS

    def test_snapshot_fact_scopes_pin_cob_dt(self):
        violations = [
            f"{path.name} :: scope `{scope}` reads {sorted(facts)} without cob_dt pin"
            for path, scope, sql, facts in iter_scopes_with(SILVER_FACT_REF)
            if not facts & INCREMENTAL_FACTS and not COB_DT_PIN.search(sql)
        ]
        assert not violations, (
            "Thiếu `cob_dt = DATE '{{ cob_dt }}'` — Silver fact là full snapshot "
            "mỗi cob_dt nên aggregate sẽ cộng chồng qua các ngày:\n  " + "\n  ".join(violations)
        )

    def test_incremental_fact_scopes_read_a_range_up_to_cob_dt(self):
        """Pin một ngày trên fact incremental = chỉ đọc giao dịch của đúng ngày đó."""
        violations = [
            f"{path.name} :: scope `{scope}` reads {sorted(facts)}"
            for path, scope, sql, facts in iter_scopes_with(SILVER_FACT_REF)
            if facts & INCREMENTAL_FACTS and (COB_DT_PIN.search(sql) or not INCREMENTAL_RANGE.search(sql))
        ]
        assert not violations, (
            "Fact incremental phải đọc khoảng partition có cận trên cob_dt "
            "(BETWEEN DATE_ADD(cob, -N) AND cob, hoặc <= cob), không pin một ngày:\n  " + "\n  ".join(violations)
        )

    def test_partition_range_covers_the_business_window(self):
        """Cận dưới khoảng partition hẹp hơn cửa sổ nghiệp vụ → lặng lẽ mất giao dịch đầu cửa sổ."""
        violations = []
        for path, scope, sql, facts in iter_scopes_with(SILVER_FACT_REF):
            if not facts & INCREMENTAL_FACTS:
                continue
            ranges = [int(n) for n in INCREMENTAL_RANGE.findall(sql) if n]
            windows = [int(n) for n in BUSINESS_WINDOW.findall(sql)]
            if ranges and windows and min(ranges) < max(windows):
                violations.append(f"{path.name} :: `{scope}` partition -{min(ranges)} < window -{max(windows)}")
        assert not violations, "\n  ".join(violations)

    def test_range_check_sees_a_too_narrow_range(self):
        sql = (
            "FROM lakehouse.silver.fact_txn_account WHERE cob_dt BETWEEN DATE_ADD(DATE '{{ cob_dt }}', -30) "
            "AND DATE '{{ cob_dt }}' AND d >= DATE_ADD(DATE '{{ cob_dt }}', -90)"
        )
        ranges = [int(n) for n in INCREMENTAL_RANGE.findall(sql) if n]
        assert min(ranges) == 30 and max(int(n) for n in BUSINESS_WINDOW.findall(sql)) == 90

    def test_gold_to_gold_scopes_pin_cob_dt(self):
        violations = [
            f"{path.name} :: scope `{scope}` reads {sorted(tables)} without cob_dt pin"
            for path, scope, sql, tables in iter_scopes_with(GOLD_REF)
            if not COB_DT_PIN.search(sql)
        ]
        assert not violations, (
            "Gold table cũng partition theo cob_dt — đọc lại mà không chốt "
            "snapshot sẽ kéo toàn bộ lịch sử vào một partition:\n  " + "\n  ".join(violations)
        )

    def test_every_snapshot_source_is_declared_for_runtime_guard(self):
        """
        Rule A ở tầng runtime: mọi Silver fact / Gold source mà SQL đọc phải
        được khai báo trong validation.require_snapshots để gold_job.py fail
        loud khi partition không tồn tại.

        Static pin (`cob_dt = DATE '...'`) một mình là chưa đủ: model neo
        dim_customer vẫn trả về đủ dòng với metric toàn 0 khi thiếu snapshot.
        """
        violations = []
        for path in gold_config_paths():
            config = yaml.safe_load(path.read_text(encoding="utf-8"))
            sql = config.get("sql", "")
            declared = set((config.get("validation") or {}).get("require_snapshots") or [])
            used = {f"silver.{t}" for t in SILVER_FACT_REF.findall(sql)}
            used |= {f"gold.{t}" for t in GOLD_REF.findall(sql)}
            undeclared = used - declared
            if undeclared:
                violations.append(f"{path.name}: {sorted(undeclared)}")
        assert not violations, (
            "Snapshot-backed source chưa khai báo trong validation.require_snapshots "
            "→ gold_job.py sẽ không fail loud khi thiếu partition:\n  " + "\n  ".join(violations)
        )


class TestRuleB_BusinessWindowIsModelSpecific:
    """
    Rule B (model-specific): rolling business window KHÔNG phải rule universal.

    ⚠ Không thêm model calendar (branch_monthly_summary) vào ROLLING_MODELS.
      Nó cố ý gom cả lịch sử theo tháng bên trong một physical snapshot.
    """

    ROLLING_MODELS: ClassVar[list[str]] = [
        "rfm_segment.yml",
        "churn_prediction.yml",
        "customer_card_summary.yml",
        "customer_transaction_summary.yml",
        "customer_360.yml",
    ]
    CALENDAR_MODELS: ClassVar[list[str]] = ["branch_monthly_summary.yml"]

    @pytest.mark.parametrize("config_name", ROLLING_MODELS)
    def test_rolling_models_keep_business_window(self, config_name):
        """
        cob_dt pin không được phép thay thế business window — hai loại thời
        gian phải cùng tồn tại (physical snapshot + business event date).
        """
        path = next(p for p in gold_config_paths() if p.name == config_name)
        sql = load_sql(path)
        assert pinned_or_bounded(sql), f"{config_name}: thiếu snapshot pin / khoảng partition (Rule A)"
        assert re.search(r"DATE_ADD\(DATE '\{\{ cob_dt \}\}'", sql), (
            f"{config_name}: mất rolling window trên business date (Rule B)"
        )

    @pytest.mark.parametrize("config_name", CALENDAR_MODELS)
    def test_calendar_models_need_pin_but_not_window(self, config_name):
        """
        Model calendar vẫn phải tuân Rule A, nhưng KHÔNG bắt buộc Rule B.
        Test này tồn tại để ghi lại chủ ý đó — không phải để nới lỏng.
        """
        path = next(p for p in gold_config_paths() if p.name == config_name)
        sql = load_sql(path)
        assert pinned_or_bounded(sql), f"{config_name}: thiếu snapshot pin / khoảng partition (Rule A)"
        assert re.search(r"YEAR\(|MONTH\(", sql), (
            f"{config_name}: không còn calendar aggregation — nếu model này đã "
            "chuyển sang rolling window thì hãy chuyển nó sang ROLLING_MODELS"
        )


class TestScopeSplitter:
    """Splitter sai thì mọi assertion ở trên đều vô giá trị — test nó trực tiếp."""

    def test_splits_ctes_and_main(self):
        sql = """
        WITH a AS (
            SELECT x FROM t1 WHERE y IN (SELECT z FROM t2)
        ),
        b AS (
            SELECT COUNT(*) FROM t3
        )
        SELECT * FROM a JOIN b ON a.x = b.x
        """
        scopes = split_sql_scopes(sql)
        assert set(scopes) == {"a", "b", "__main__"}
        assert "t1" in scopes["a"] and "t2" in scopes["a"]
        assert "t3" in scopes["b"]
        assert "t1" not in scopes["__main__"]
        assert "JOIN b" in scopes["__main__"]

    def test_does_not_treat_cast_as_cte(self):
        sql = "SELECT CAST(x AS DATE) AS d, COUNT(y) FROM t GROUP BY CAST(x AS DATE)"
        scopes = split_sql_scopes(sql)
        assert set(scopes) == {"__main__"}
        assert "FROM t" in scopes["__main__"]

    def test_detects_fanout_in_unsplit_query(self):
        sql = """
        SELECT c.customer_id
        FROM lakehouse.silver.dim_customer c
        LEFT JOIN lakehouse.silver.fact_txn_account t ON c.customer_id = t.customer_id
        LEFT JOIN lakehouse.silver.fact_card_txn ct ON c.customer_id = ct.customer_id
        GROUP BY c.customer_id
        """
        scopes = split_sql_scopes(sql)
        facts = set(SILVER_FACT_REF.findall(scopes["__main__"]))
        assert facts == {"fact_txn_account", "fact_card_txn"}


# ---------------------------------------------------------------------------
# Wave 4 — chống regression về session-dependent date bucketing
# ---------------------------------------------------------------------------

# Cột timestamp mang nghĩa SỰ KIỆN NGHIỆP VỤ. Ngày/tháng nghiệp vụ derive từ
# chúng phải đi qua business timezone tường minh.
EVENT_TIMESTAMP_COLUMNS = ("txn_date", "transaction_date", "interaction_date")
BUSINESS_TZ = "Asia/Ho_Chi_Minh"


class TestBusinessDateDerivationIsExplicit:
    """
    Hợp đồng: instant = UTC, business date = derive TƯỜNG MINH.

    `CAST(<event_ts> AS DATE)` trần bucket theo session timezone của engine.
    Đó chính là bug đã làm Spark (session ICT) và Trino (UTC) lệch 29,2% số
    dòng trên cùng dữ liệu Iceberg. Sau migration, session của cả hai engine là
    UTC, nên CAST trần sẽ âm thầm đổi nghĩa sang ngày UTC — im lặng và sai.
    """

    def test_no_naive_cast_on_event_timestamps(self):
        violations = []
        for path in gold_config_paths():
            sql = load_sql(path)
            for col in EVENT_TIMESTAMP_COLUMNS:
                for m in re.finditer(rf"CAST\(\s*(\w+\.)?{col}\s+AS\s+DATE\)", sql):
                    violations.append(f"{path.name}: {m.group(0)}")
        assert not violations, (
            "CAST trần trên event timestamp — bucket theo session timezone.\n"
            f"Dùng: CAST(from_utc_timestamp(<ts>, '{BUSINESS_TZ}') AS DATE)\n  " + "\n  ".join(violations)
        )

    def test_no_naive_time_part_on_event_timestamps(self):
        """
        Giờ / thứ / tháng nghiệp vụ cũng là giờ VN, không riêng ngày.
        `HOUR(txn_date)` trần dưới session UTC là giờ UTC: night_flag từng đo
        06:00–12:59 giờ VN thay vì 23:00–05:59.
        """
        parts = r"HOUR|MINUTE|DAYOFWEEK|WEEKDAY|DAYOFMONTH|DAY|MONTH|YEAR|DATE_FORMAT|DATE_TRUNC|EXTRACT"
        violations = []
        for path in gold_config_paths():
            sql = load_sql(path)
            for col in EVENT_TIMESTAMP_COLUMNS:
                pattern = rf"\b({parts})\(\s*(?:\w+\s+FROM\s+|'\w+'\s*,\s*)?(\w+\.)?{col}\b"
                for m in re.finditer(pattern, sql, re.IGNORECASE):
                    violations.append(f"{path.name}: {m.group(0)}")
        assert not violations, (
            "Trích giờ/thứ/tháng trần trên event timestamp — ra giờ UTC, không phải giờ nghiệp vụ.\n"
            f"Dùng: HOUR(from_utc_timestamp(<ts>, '{BUSINESS_TZ}'))\n  " + "\n  ".join(violations)
        )

    def test_business_date_uses_explicit_timezone(self):
        """Mọi derive ngày từ event timestamp phải nêu rõ business timezone."""
        missing = []
        for path in gold_config_paths():
            sql = load_sql(path)
            uses_event_date = any(
                re.search(rf"from_utc_timestamp\(\s*(\w+\.)?{col}", sql) for col in EVENT_TIMESTAMP_COLUMNS
            )
            if uses_event_date and BUSINESS_TZ not in sql:
                missing.append(path.name)
        assert not missing, f"derive ngày mà không nêu business timezone: {missing}"

    def test_cob_dt_is_never_derived_from_event_timestamp(self):
        """
        cob_dt là orchestration date. Timezone migration KHÔNG được làm nó trở
        thành hàm của event timestamp.
        """
        violations = []
        for path in gold_config_paths():
            sql = load_sql(path)
            for m in re.finditer(r"(\w+)\s+AS\s+cob_dt", sql, re.IGNORECASE):
                expr_start = max(0, m.start() - 80)
                if any(c in sql[expr_start : m.start()] for c in EVENT_TIMESTAMP_COLUMNS):
                    violations.append(f"{path.name}: {m.group(0)}")
        assert not violations, f"cob_dt derive từ event timestamp: {violations}"

    def test_aggregates_on_instants_stay_untouched(self):
        """
        MAX(txn_date) là INSTANT (dùng cho recency/last_txn), KHÔNG phải business
        date — không được bọc timezone conversion vào. Chỉ ngày/tháng nghiệp vụ
        mới derive.
        """
        wrong = []
        for path in gold_config_paths():
            sql = load_sql(path)
            for col in EVENT_TIMESTAMP_COLUMNS:
                if re.search(rf"MAX\(\s*from_utc_timestamp\(\s*(\w+\.)?{col}", sql):
                    wrong.append(f"{path.name}: MAX(from_utc_timestamp({col}...))")
        assert not wrong, (
            f"instant aggregate bị bọc timezone conversion — đó là business-date semantics áp nhầm lên instant: {wrong}"
        )


RANKING_HEAD = re.compile(r"\b(?:ROW_NUMBER|RANK|DENSE_RANK|NTILE)\s*\([^)]*\)\s*OVER\s*\(", re.IGNORECASE)


def ranking_windows(sql: str) -> list[str]:
    """Nội dung OVER ( ... ) của mọi hàm xếp hạng — quét cân bằng ngoặc, vì PARTITION BY
    có thể chứa `CAST(... AS DATE)` làm regex không tham lam dừng sớm."""
    out = []
    for m in RANKING_HEAD.finditer(sql):
        depth, i = 1, m.end()
        while i < len(sql) and depth:
            depth += sql[i] == "("
            depth -= sql[i] == ")"
            i += 1
        out.append(sql[m.end() : i - 1])
    return out


def _order_keys(window: str) -> int:
    order = re.search(r"ORDER\s+BY\s+(.*)$", window, re.IGNORECASE | re.DOTALL)
    if not order:
        return 0
    depth, keys = 0, 1
    for ch in order.group(1):
        depth += ch == "("
        depth -= ch == ")"
        keys += ch == "," and depth == 0
    return keys


class TestDeterministicRanking:
    """
    Xếp hạng chỉ theo một số đo (COUNT, amount…) cho kết quả không tất định khi hoà:
    customer_360.primary_channel đổi ở 621/10.000 khách chỉ vì thứ tự vật lý của dữ liệu
    đổi (2026-09-30). Mọi ROW_NUMBER/RANK/NTILE phải có ít nhất một khoá phá hoà.
    """

    def test_every_ranking_has_a_tie_breaker(self):
        violations = []
        for path in gold_config_paths():
            sql = re.sub(r"--[^\n]*", "", load_sql(path))
            for window in ranking_windows(sql):
                if _order_keys(window) < 2:
                    violations.append(f"{path.name}: OVER ({' '.join(window.split())[:120]})")
        assert not violations, "\n  ".join(violations)

    def test_scanner_sees_a_single_key_ranking(self):
        sql = "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(txn_id) DESC) AS rn"
        assert [_order_keys(w) for w in ranking_windows(sql)] == [1]

    def test_scanner_reads_past_cast_in_partition_by(self):
        sql = "ROW_NUMBER() OVER (PARTITION BY CAST(d AS DATE) ORDER BY n DESC, channel) AS rn"
        assert [_order_keys(w) for w in ranking_windows(sql)] == [2]
