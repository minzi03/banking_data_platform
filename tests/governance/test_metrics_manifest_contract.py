"""
Contract tests cho docs/evidence/metrics-manifest.yaml.

Manifest là EVIDENCE CONTRACT, không phải file chứa số. Test này enforce cái
contract đó ngay từ lúc mọi value còn null, để:

  1. Không ai gõ tay số vào manifest canonical (chỉ script được điền).
  2. Mọi metric runtime đều có query tương ứng trong metrics-manifest.sql.
  3. Mọi invariant và readme_binding trỏ tới node có thật.
  4. Metric mơ hồ buộc phải có `definition`.

Chạy trong CI, không cần Spark/Trino.
"""

import re
from pathlib import Path

import pytest
import yaml

EVIDENCE_DIR = Path(__file__).resolve().parents[2] / "docs" / "evidence"
MANIFEST_PATH = EVIDENCE_DIR / "metrics-manifest.yaml"
SQL_PATH = EVIDENCE_DIR / "metrics-manifest.sql"

VALID_METRIC_TYPES = {"static", "runtime", "manual"}


@pytest.fixture(scope="module")
def manifest() -> dict:
    return yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def sql_ids() -> set[str]:
    text = SQL_PATH.read_text(encoding="utf-8")
    return set(re.findall(r"^--@id\s+(\S+)", text, re.MULTILINE))


def resolve(root: dict, dotted: str):
    """Đi theo đường dẫn `a.b.c`; trả về sentinel nếu không tồn tại."""
    node = root
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return ...
        node = node[part]
    return node


def iter_metric_nodes(root, path=""):
    """Yield (path, node) cho mọi dict có key `metric_type`."""
    if isinstance(root, dict):
        if "metric_type" in root:
            yield path, root
        for key, child in root.items():
            yield from iter_metric_nodes(child, f"{path}.{key}" if path else key)


# ---------------------------------------------------------------------------
# Cấu trúc cơ bản
# ---------------------------------------------------------------------------

class TestManifestStructure:
    def test_files_exist(self):
        assert MANIFEST_PATH.exists(), "thiếu metrics-manifest.yaml"
        assert SQL_PATH.exists(), "thiếu metrics-manifest.sql"

    def test_schema_version_present(self, manifest):
        assert manifest["manifest"]["schema_version"] == "1.1"

    def test_required_top_level_sections(self, manifest):
        for section in ("manifest", "metrics", "invariants", "readme_bindings"):
            assert section in manifest, f"thiếu section: {section}"

    def test_snapshot_records_cob_dt_per_layer(self, manifest):
        """
        Một giá trị cob_dt chung là không đủ: partial rebuild có thể để Bronze
        ở D2 còn Gold ở D1, và metric trộn hai layer lệch nhau là evidence sai.
        """
        snapshot = manifest["manifest"]["snapshot"]
        for key in (
            "requested_cob_dt",
            "bronze_max_cob_dt",
            "silver_max_cob_dt",
            "gold_max_cob_dt",
            "layers_aligned",
        ):
            assert key in snapshot, f"snapshot thiếu {key}"


# ---------------------------------------------------------------------------
# Trạng thái skeleton — chống gõ tay số vào manifest
# ---------------------------------------------------------------------------

class TestManifestStateMachine:
    def test_status_is_valid(self, manifest):
        assert manifest["manifest"]["verification"]["status"] in {
            "pending", "verified", "warning", "failed"
        }

    def test_pending_manifest_has_no_runtime_values(self, manifest):
        """
        Khi status = pending, mọi metric runtime phải còn null.

        Đây là chốt chặn quan trọng nhất của file này: nó khiến việc gõ tay một
        con số "cho có" vào manifest canonical trở thành CI failure, thay vì
        lặng lẽ trở thành evidence.
        """
        if manifest["manifest"]["verification"]["status"] != "pending":
            pytest.skip("manifest đã được sinh — kiểm tra này chỉ áp cho skeleton")

        filled = [
            path
            for path, node in iter_metric_nodes(manifest["metrics"])
            if node.get("metric_type") == "runtime" and node.get("value") is not None
        ]
        assert not filled, (
            "metric runtime có value trong khi status=pending "
            f"(gõ tay?): {filled}"
        )

    def test_pending_manifest_has_no_generated_timestamp(self, manifest):
        if manifest["manifest"]["verification"]["status"] == "pending":
            assert manifest["manifest"]["runtime"]["generated_at_utc"] is None

    def test_build_provenance_block_exists(self, manifest):
        """
        Số liệu phải gắn được với revision sinh ra nó: nhìn '2.3M transactions'
        là trả lời được ngay đo từ commit nào, snapshot nào.
        """
        build = manifest["manifest"]["build"]
        for key in ("git_commit", "git_branch", "git_dirty", "portfolio_release"):
            assert key in build, f"build thiếu {key}"
        if manifest["manifest"]["verification"]["status"] == "pending":
            assert build["git_commit"] is None

    def test_manual_metrics_may_carry_values_while_pending(self, manifest):
        """
        Ngoại lệ có chủ ý: cdc_freshness là benchmark đo tay, không query được.
        Nó GIỮ giá trị ngay cả khi status=pending, và script phải không ghi đè.
        """
        freshness = manifest["metrics"]["cdc_freshness"]
        assert freshness["metric_type"] == "manual"
        assert freshness["trials_seconds"], "benchmark thủ công không được để rỗng"
        assert freshness["sample_size"] == len(freshness["trials_seconds"])

    def test_freshness_number_is_interpretable_before_it_counts_as_valid(self, manifest):
        """
        `revalidate_required: false` là lời khẳng định "số này dùng được".
        Một con số freshness chỉ dùng được khi biết nó đo thế nào và ở cadence
        nào — 409.8s ở cadence 600s và 409.8s ở cadence 60s là hai hệ thống
        khác nhau. Nên trước khi được hạ cờ, khối phải mang đủ ngữ cảnh.

        Test này thay cho assert `revalidate_required is True` của v1.0: mục
        đích cũ (ép đo lại) đã đạt; mục đích bây giờ là không cho hạ cờ trên
        một con số trần trụi.
        """
        freshness = manifest["metrics"]["cdc_freshness"]
        if freshness.get("revalidate_required", True):
            return

        required = (
            "methodology",
            "measured_on_release",
            "measured_at_utc",
            "consolidation_cadence_seconds",
            "phase_sampling",
        )
        missing = [k for k in required if not freshness.get(k)]
        assert not missing, (
            f"cdc_freshness hạ revalidate_required nhưng thiếu ngữ cảnh: {missing}"
        )

        stats = (freshness["min_seconds"], freshness["median_seconds"], freshness["max_seconds"])
        assert stats[0] <= stats[1] <= stats[2], f"min/median/max không nhất quán: {stats}"
        assert freshness["observed_count"] == freshness["sample_size"], (
            "có trial timeout — không được công bố median trên mẫu thiếu"
        )

    def test_superseded_freshness_claim_is_kept_and_explained(self, manifest):
        """Số cũ bị thay thế phải còn dấu vết, kèm lý do vì sao không so được."""
        freshness = manifest["metrics"]["cdc_freshness"]
        superseded = freshness.get("superseded_claim")
        if superseded is None:
            return
        assert superseded["median_seconds"] != freshness["median_seconds"]
        assert superseded.get("why_not_comparable"), (
            "giữ số cũ mà không nói vì sao không so được thì chỉ gây hiểu nhầm"
        )


# ---------------------------------------------------------------------------
# Chất lượng định nghĩa metric
# ---------------------------------------------------------------------------

class TestMetricNodeQuality:
    def test_metric_types_are_valid(self, manifest):
        bad = [
            (path, node["metric_type"])
            for path, node in iter_metric_nodes(manifest["metrics"])
            if node["metric_type"] not in VALID_METRIC_TYPES
        ]
        assert not bad, f"metric_type không hợp lệ: {bad}"

    def test_every_metric_node_has_provenance(self, manifest):
        missing = [
            path
            for path, node in iter_metric_nodes(manifest["metrics"])
            if not node.get("provenance") and not node.get("definition")
        ]
        assert not missing, (
            "metric không nói rõ lấy từ đâu (provenance) hay đếm cái gì "
            f"(definition): {missing}"
        )

    def test_static_metrics_declare_expected_value(self, manifest):
        """
        Static metric phải CÓ KEY `declared` = con số README đang dùng, để
        script so được và phát hiện drift.

        `declared: null` là hợp lệ và có nghĩa "README chưa claim metric này" —
        nhưng key phải hiện diện, để phân biệt với việc quên khai báo.
        """
        missing = [
            path
            for path, node in iter_metric_nodes(manifest["metrics"])
            if node.get("metric_type") == "static" and "declared" not in node
        ]
        assert not missing, f"static metric thiếu `declared`: {missing}"

    def test_ambiguous_metrics_carry_definition(self, manifest):
        """
        Ba metric này đã được chứng minh là mơ hồ khi audit repo — mỗi cái có
        ít nhất hai cách đếm hợp lý. Bắt buộc phải có definition.
        """
        for path in (
            "platform.automated_tests.test_functions",
            "platform.automated_tests.collected_pytest_nodes",
            "platform.docker_services.long_running",
            "platform.docker_services.one_shot",
            "platform.airflow_dag_objects",
            "source.source_datasets",
            "transaction_scale.curated_financial_transactions",
        ):
            node = resolve(manifest["metrics"], path)
            assert node is not ..., f"thiếu node {path}"
            assert node.get("definition"), f"{path} phải có definition"


# ---------------------------------------------------------------------------
# Liên kết manifest ↔ SQL ↔ README
# ---------------------------------------------------------------------------

class TestCrossReferences:
    def test_sql_has_id_markers(self, sql_ids):
        assert len(sql_ids) >= 15, f"quá ít --@id trong SQL bundle: {len(sql_ids)}"

    def test_reconciliation_queries_exist(self, manifest, sql_ids):
        """Mỗi reconciliation có tolerance đều phải có query tương ứng."""
        for name, node in manifest["metrics"]["gold"]["reconciliation"].items():
            if not isinstance(node, dict) or node.get("status") == "not_applicable":
                continue
            assert f"gold.reconciliation.{name}" in sql_ids, (
                f"reconciliation `{name}` không có query trong metrics-manifest.sql"
            )

    def test_readme_bindings_resolve(self, manifest):
        unresolved = [
            b["manifest_path"]
            for b in manifest["readme_bindings"]
            if resolve(manifest, b["manifest_path"]) is ...
        ]
        assert not unresolved, (
            f"readme_binding trỏ tới node không tồn tại: {unresolved}"
        )

    def test_invariants_are_well_formed(self, manifest):
        valid_ops = {"eq", "ne", "lt", "le", "gt", "ge"}
        for inv_id, inv in manifest["invariants"].items():
            assert inv.get("severity") in {"error", "warn"}, (
                f"{inv_id}: severity phải là error|warn — đây là thứ phân biệt "
                "blocking invariant với observation"
            )
            assert inv.get("operator") in valid_ops, f"{inv_id}: operator không hợp lệ"
            assert "expected" in inv or "compare_to" in inv, (
                f"{inv_id}: cần expected hoặc compare_to"
            )

    def test_blocking_and_warning_invariants_both_exist(self, manifest):
        """
        Contract phải phân biệt được hai loại kết luận. Nếu mọi invariant đều là
        error thì một drift README vô hại cũng chặn được cả evidence pipeline.
        """
        severities = {inv["severity"] for inv in manifest["invariants"].values()}
        assert severities == {"error", "warn"}

    def test_readme_drift_is_warn_not_error(self, manifest):
        assert manifest["invariants"]["static_metrics_match_declared"]["severity"] == "warn"

    def test_p0_regressions_are_covered_by_invariants(self, manifest):
        """
        Hai bug P0 vừa sửa phải có invariant vĩnh viễn trong manifest, không chỉ
        trong test suite. Test suite bảo vệ SQL; manifest bảo vệ dữ liệu thật
        sau mỗi rebuild.
        """
        ids = set(manifest["invariants"])
        assert "gold_grain_no_duplicates" in ids, "① fan-out chưa có invariant"
        assert "churn_reconciles_amount" in ids, "① reconciliation chưa có invariant"
        assert "silver_fact_snapshot_not_duplicated" in ids, "② chưa có invariant"
        for inv_id in ("gold_grain_no_duplicates", "churn_reconciles_amount",
                       "silver_fact_snapshot_not_duplicated"):
            assert manifest["invariants"][inv_id]["severity"] == "error", (
                f"{inv_id} phải là blocking — nó bảo vệ bug P0 đã từng xảy ra"
            )

    def test_serving_contract_is_blocking(self, manifest):
        """
        Sau migration huong C, blocking check la hop dong PHUC VU
        (dung snapshot, du object), khong phai "bang co cu khong".
        """
        assert manifest["invariants"]["serving_snapshot_alignment"]["severity"] == "error"
        assert manifest["invariants"]["serving_objects_complete"]["severity"] == "error"


# ---------------------------------------------------------------------------
# Không encode claim chưa implement
# ---------------------------------------------------------------------------

class TestNoUnimplementedClaims:
    def test_cdc_watermark_state_is_honest(self, manifest):
        """
        Chốt chặn chống README drift quay lại: hai field này chỉ được đặt true
        khi P1 thực sự persist Kafka metadata vào valid Bronze CDC.
        Hiện tại cdc_dlq.py drop kafka metadata và meta.cdc_watermark chỉ có
        table_name → cả hai phải là false.
        """
        wm = manifest["metrics"]["cdc"]["consolidation_watermark"]
        assert wm["implementation"] == "timestamp_plus_spark_batch_id"
        assert wm["partition_aware"] is False
        assert wm["kafka_offsets_persisted_in_valid_bronze"] is False

    def test_superseded_transaction_claim_is_recorded_not_reused(self, manifest):
        """
        4.6M được giữ lại như ngữ cảnh lịch sử, KHÔNG được dùng làm value.
        """
        node = manifest["metrics"]["transaction_scale"]["curated_financial_transactions"]
        assert node["superseded_claim"]["value"] == 4_600_000
        assert node["superseded_claim"]["suspected_cause"]
        assert node["value"] != node["superseded_claim"]["value"], (
            "4.6M đã bị bác bỏ — không được quay lại làm giá trị verify"
        )

    def test_legacy_ctas_retirement_is_tracked(self, manifest):
        """Issue ⑤ van phai hien dien trong evidence toi khi legacy duoc retire."""
        assert "legacy_gold_current_objects" in manifest["metrics"]["gold"]
        assert "current_snapshot_alignment" in manifest["metrics"]["serving"]


# ---------------------------------------------------------------------------
# Catalog naming — phát hiện khi runtime-validate lần đầu
# ---------------------------------------------------------------------------

class TestCatalogNaming:
    """
    CÙNG một Iceberg warehouse, HAI tên catalog tuỳ engine:
        Spark → `lakehouse`  (spark-defaults.conf, dùng trong mọi ETL YAML)
        Trino → `iceberg`    (tên file init_trino/catalog/iceberg.properties)

    Lần chạy verify đầu tiên fail toàn bộ với "Catalog 'lakehouse' not found"
    vì bundle bê nguyên tên catalog phía Spark. Các test dưới đây khoá bài học đó.
    """

    def test_both_catalog_names_declared(self, manifest):
        env = manifest["manifest"]["environment"]
        assert env["spark_catalog"] == "lakehouse"
        assert env["trino_catalog"] == "iceberg"

    def test_sql_bundle_uses_catalog_placeholder(self):
        sql = SQL_PATH.read_text(encoding="utf-8")
        code = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        )
        assert ":catalog." in code, "bundle phải dùng placeholder :catalog."
        assert "lakehouse." not in code, (
            "hard-code `lakehouse.` trong SQL sẽ fail trên Trino "
            "(\"Catalog 'lakehouse' not found\") — dùng :catalog."
        )

    def test_trino_catalog_matches_properties_filename(self):
        """
        Tên catalog của Trino = tên file .properties. Đổi tên file mà quên sửa
        manifest thì mọi query lại fail như lần đầu.
        """
        catalog_dir = Path(__file__).resolve().parents[2] / "docker" / "init_trino" / "catalog"
        names = {p.stem for p in catalog_dir.glob("*.properties")}
        manifest_data = yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8"))
        assert manifest_data["manifest"]["environment"]["trino_catalog"] in names
