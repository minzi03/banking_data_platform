"""
Unit tests cho scripts/generate_metrics_manifest.py — chạy KHÔNG cần stack.

Mục tiêu: khi rebuild xong, generator đã được kiểm chứng sẵn về logic
(template expansion, invariant evaluation, promote/không-promote), nên lần chạy
đầu chỉ còn phải sửa mismatch giữa SQL và schema thật — không phải debug đồng
thời cả tooling lẫn dữ liệu.

Trino được thay bằng FakeTrinoClient.
"""

import importlib.util
import re
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_generator():
    path = PROJECT_ROOT / "scripts" / "generate_metrics_manifest.py"
    spec = importlib.util.spec_from_file_location("generate_metrics_manifest", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gen = _load_generator()


def _skeleton(contract: dict) -> dict:
    """
    Dung lai trang thai SKELETON tu contract.

    Cac test ve hop dong phai doc lap voi viec canonical manifest da duoc
    promote hay chua. Sau khi generator chay that, file canonical chua gia tri
    da do va scope=full — test nao gia dinh "moi thu con null" se hong, dung
    luc pipeline dang hoat dong tot. Do la phu thuoc sai.
    """
    m = yaml.safe_load(yaml.safe_dump(contract))
    for _path, node in gen._iter_metric_nodes(m["metrics"]):
        if node.get("metric_type") == "manual":
            continue
        for key in list(node):
            if key not in {"metric_type", "provenance", "definition", "declared",
                           "required_keys", "note", "calculation", "superseded_claim",
                           "tolerance", "status", "one_shot_services",
                           "definition_binding"}:
                node[key] = None
    return m


@pytest.fixture(scope="module")
def contract() -> dict:
    return gen.load_contract()


class FakeTrinoClient:
    """Trino giả: trả row theo pattern trong SQL, hoặc ném lỗi để test error path."""

    def __init__(self, responses: dict | None = None, fail_on: str | None = None):
        self.responses = responses or {}
        self.fail_on = fail_on
        self.executed: list[str] = []

    def query(self, sql: str):
        self.executed.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("line 3:8: Column 'nope' cannot be resolved")
        for needle, rows in self.responses.items():
            if needle in sql:
                return rows
        return [{"value": 0}]


# ---------------------------------------------------------------------------
# load_contract / validate_contract
# ---------------------------------------------------------------------------

class TestContractValidation:
    def test_shipped_contract_is_valid(self, contract):
        assert gen.validate_contract(contract) == []

    def test_detects_invariant_pointing_at_missing_metric(self, contract):
        broken = yaml.safe_load(yaml.safe_dump(contract))
        broken["invariants"]["bogus"] = {
            "metric": "metrics.does.not.exist", "operator": "eq",
            "expected": 0, "severity": "error",
        }
        errors = gen.validate_contract(broken)
        assert any("bogus" in e for e in errors)

    def test_detects_bad_severity(self, contract):
        broken = yaml.safe_load(yaml.safe_dump(contract))
        broken["invariants"]["scd2_no_duplicate_current_keys"]["severity"] = "critical"
        assert any("severity" in e for e in gen.validate_contract(broken))

    def test_detects_broken_readme_binding(self, contract):
        broken = yaml.safe_load(yaml.safe_dump(contract))
        broken["readme_bindings"].append(
            {"readme_claim": "x", "manifest_path": "metrics.nope.value"}
        )
        assert any("readme_binding" in e for e in gen.validate_contract(broken))


# ---------------------------------------------------------------------------
# collect_repo_metrics — static sinh từ repo, không gõ tay
# ---------------------------------------------------------------------------

class TestRepoCollectors:
    def test_source_datasets_excludes_template_by_semantics(self, contract):
        """
        Glob thô ra 17 file; đúng phải là 16 workload.
        templates/source_registry.yml bị loại vì thiếu contract shape
        (source+target+load+sql), KHÔNG phải vì blacklist tên file.
        """
        assert gen._bronze_ingestion_workloads(contract) == 16
        assert len(list((PROJECT_ROOT / "code_etl/bronze").glob("*/*.yml"))) == 17

    def test_debezium_topics_reads_config_not_docstring(self, contract):
        """Docstring đầu register_connectors.py ghi 8/3/5 — sai. Config là 6/3/3."""
        assert gen._debezium_topics(contract) == 12
        assert gen._debezium_connectors(contract) == 3

    def test_silver_dimension_split(self):
        assert gen._silver_dims(None) == 8
        assert gen._silver_dims("scd_type1") == 6
        assert gen._silver_dims("scd_type2") == 2

    def test_dq_check_types(self, contract):
        assert gen._dq_check_types(contract) == 8

    def test_gold_ddl_tables(self, contract):
        """
        10 bang lich su. 8 CTAS `*_current` da bi RETIRE khoi DDL — chung gio la
        dbt serving model. Neu con so nay tang lai gan 18, nghia la co ai do
        them lai CTAS vao DDL → dual ownership quay lai.
        """
        assert gen._gold_ddl_tables(contract) == 10

    def test_docker_services_three_way_split(self, contract):
        """24 = 20 long-running + 4 one-shot. Không còn tranh cãi 23 hay 24."""
        defined, long_running, one_shot = gen._compose_services(contract)
        assert defined == 24
        assert one_shot == 4
        assert long_running == defined - one_shot == 20

    def test_airflow_files_vs_objects_differ(self, contract):
        """cdc_streaming_dag.py định nghĩa 2 DAG — hai metric khác nhau."""
        assert gen._airflow_dag_files(contract) == 16
        assert gen._airflow_dag_objects(contract) == 17

    def test_collect_repo_metrics_fills_every_static_node(self, contract):
        collected = gen.collect_repo_metrics(contract)
        static_paths = {
            f"{path}.value"
            for path, node in gen._iter_metric_nodes(contract["metrics"])
            if node.get("metric_type") == "static"
        }
        assert static_paths <= set(collected), (
            f"static metric không có collector: {sorted(static_paths - set(collected))}"
        )


# ---------------------------------------------------------------------------
# render_query_bundle
# ---------------------------------------------------------------------------

class TestQueryBundle:
    def test_template_expands_per_table(self, contract):
        queries = gen.render_query_bundle(contract, "2026-09-06")
        grain = [q for q in queries if q.id.startswith("gold.grain_checks.")]
        assert len(grain) == 9
        assert not any(q.id.endswith(".TEMPLATE") for q in queries)
        assert all("{table}" not in q.sql for q in queries)

    def test_cob_dt_substituted_everywhere(self, contract):
        queries = gen.render_query_bundle(contract, "2026-09-06")
        assert not any(":cob_dt" in q.sql for q in queries)
        assert any("2026-09-06" in q.sql for q in queries)

    def test_phase_order_is_cheap_first(self, contract):
        """
        Snapshot D3 không tồn tại thì phải dừng trước 30 query còn lại,
        nên phase phải tăng dần và snapshot check nằm ở phase sớm.
        """
        queries = gen.render_query_bundle(contract, "2026-09-06")
        assert [q.phase for q in queries] == sorted(q.phase for q in queries)
        snapshot_phase = next(q.phase for q in queries if q.id.startswith("snapshot."))
        recon_phase = next(q.phase for q in queries if "reconciliation" in q.id)
        assert snapshot_phase < recon_phase

    def test_stop_after_phase_short_circuits(self, contract):
        queries = gen.render_query_bundle(contract, "2026-09-06")
        client = FakeTrinoClient()
        gen.collect_trino_metrics(queries, client, "2026-09-06", stop_after_phase=2)
        assert len(client.executed) < len(queries)


# ---------------------------------------------------------------------------
# Error reporting
# ---------------------------------------------------------------------------

class TestQueryErrorReporting:
    def test_failure_names_query_id_and_sql(self, contract):
        """
        Lần chạy đầu chắc chắn lộ mismatch SQL ↔ schema thật. Lỗi phải chỉ thẳng
        --@id thay vì bắt đi tìm tay trong bundle.
        """
        queries = gen.render_query_bundle(contract, "2026-09-06")
        # Fake client chỉ nhìn thấy SQL, không thấy query id — nên marker phải
        # là một mảnh SQL có thật, ở đây là self-join của overlapping_intervals.
        client = FakeTrinoClient(fail_on="b.effective_from <= a.effective_to")
        with pytest.raises(gen.MetricQueryError) as exc:
            gen.collect_trino_metrics(queries, client, "2026-09-06")
        message = str(exc.value)
        assert "overlapping_intervals" in message, (
            "thông báo lỗi phải chỉ thẳng --@id, không bắt đi dò tay trong bundle"
        )
        assert "2026-09-06" in message
        assert "Rendered SQL" in message
        assert "cannot be resolved" in message


# ---------------------------------------------------------------------------
# evaluate_invariants
# ---------------------------------------------------------------------------

def _verified_manifest(contract: dict) -> dict:
    """Manifest đã điền đủ giá trị hợp lệ để mọi blocking invariant xanh."""
    m = yaml.safe_load(yaml.safe_dump(contract))
    m["manifest"]["build"].update(
        {"git_commit": "abc123", "git_branch": "main", "git_dirty": False}
    )
    snap = m["manifest"]["snapshot"]
    snap.update({
        "requested_cob_dt": "2026-09-06", "bronze_max_cob_dt": "2026-09-06",
        "silver_max_cob_dt": "2026-09-06", "gold_max_cob_dt": "2026-09-06",
        "bronze_partition_exists": True, "silver_partition_exists": True,
        "gold_partition_exists": True, "layers_aligned": True,
    })
    for dim in m["metrics"]["silver"]["scd2"].values():
        dim.update({
            "duplicate_current_keys": 0, "current_rows": 100,
            "current_distinct_business_keys": 100, "overlapping_intervals": 0,
        })
    for name, node in m["metrics"]["silver"]["snapshot_rows"].items():
        key = next(k for k in node if k.startswith("distinct_"))
        node["rows"], node[key] = 500, 500
        assert name
    for node in m["metrics"]["gold"]["grain_checks"].values():
        node.update({"rows": 100, "distinct_customer_id": 100, "duplicate_customer_ids": 0})
    for name in ("churn_vs_transaction_summary_30d",
                 "churn_count_vs_transaction_summary_30d",
                 "rfm_monetary_recompute_90d"):
        m["metrics"]["gold"]["reconciliation"][name]["mismatched_customers"] = 0
    # Cross-engine calendar-bucket reconciliation (timezone migration)
    m["metrics"]["gold"]["reconciliation"]["branch_monthly_recompute"]["mismatched_buckets"] = 0
    m["metrics"]["platform"]["business_vs_utc_date_rows"]["divergent_date_rows"] = 350099
    for node in m["metrics"]["cdc"]["current_state"].values():
        node["duplicate_keys"] = 0
    m["metrics"]["gold"]["legacy_gold_current_objects"]["value"] = 0
    m["metrics"]["serving"]["gold_objects_declared"]["value"] = 19
    m["metrics"]["serving"]["trino"]["visible_gold_objects"]["value"] = 19
    m["metrics"]["serving"]["trino"]["mart_customer_360_current_visible"]["value"] = 0
    m["metrics"]["serving"]["objects_present"]["value"] = 9
    m["metrics"]["serving"]["current_snapshot_alignment"]["value"] = 0
    return m


class TestInvariantEvaluation:
    def test_fully_populated_manifest_passes(self, contract):
        errors, _, _ = gen.evaluate_invariants(_verified_manifest(contract))
        assert errors == []

    def test_unmeasured_metric_is_failure_not_pass(self, contract):
        """
        Skeleton toàn null KHÔNG được coi là verified. None phải FAIL, nếu không
        một manifest chưa đo gì cũng promote được.
        """
        errors, _, _ = gen.evaluate_invariants(_skeleton(contract))
        assert errors, "manifest toàn null mà không có error → logic sai"

    def test_wildcard_expands_over_all_children(self, contract):
        m = _verified_manifest(contract)
        m["metrics"]["gold"]["grain_checks"]["rfm_segment"]["duplicate_customer_ids"] = 3
        errors, _, _ = gen.evaluate_invariants(m)
        assert any("rfm_segment" in e and "gold_grain_no_duplicates" in e for e in errors)

    def test_compare_to_mirrors_wildcard_position(self, contract):
        """`silver.scd2.*.current_rows` vs `silver.scd2.*.current_distinct...`."""
        m = _verified_manifest(contract)
        m["metrics"]["silver"]["scd2"]["dim_account"]["current_rows"] = 101
        errors, _, _ = gen.evaluate_invariants(m)
        assert any("dim_account" in e and "current_rows" in e for e in errors)
        assert not any("dim_customer" in e for e in errors)

    def test_snapshot_duplication_detected(self, contract):
        """② — rows > distinct trong cùng partition."""
        m = _verified_manifest(contract)
        m["metrics"]["silver"]["snapshot_rows"]["fact_txn_account"]["rows"] = 1000
        errors, _, _ = gen.evaluate_invariants(m)
        assert any("silver_fact_snapshot_not_duplicated" in e for e in errors)

    def test_serving_misalignment_blocks(self, contract):
        """
        Thay cho test stale cu. Sau migration huong C, cau hoi chan khong con
        la "bang co cu khong" ma la "serving co phuc vu dung snapshot khong".
        """
        m = _verified_manifest(contract)
        m["metrics"]["serving"]["current_snapshot_alignment"]["value"] = 3
        errors, _, _ = gen.evaluate_invariants(m)
        assert any("serving_snapshot_alignment" in e for e in errors)

    def test_incomplete_serving_layer_blocks(self, contract):
        m = _verified_manifest(contract)
        m["metrics"]["serving"]["objects_present"]["value"] = 7
        errors, _, _ = gen.evaluate_invariants(m)
        assert any("serving_objects_complete" in e for e in errors)

    def test_legacy_ctas_reappearing_is_error(self, contract):
        """
        NANG TU WARN LEN ERROR sau khi migration hoan tat.

        Giai doan chuyen tiep legacy con la fallback nen chi WARN. Gio
        dbt_serving_publish DAG da duoc chung minh ca positive lan negative
        path, legacy khong con vai tro nao — neu chung xuat hien lai thi hoac
        DDL bi them lai, hoac co duong bootstrap khac dang tao chung.
        """
        m = _verified_manifest(contract)
        m["metrics"]["gold"]["legacy_gold_current_objects"]["value"] = 8
        errors, _, _ = gen.evaluate_invariants(m)
        assert any("legacy_gold_current_retired" in e for e in errors)

    def test_partial_rebuild_blocks_verification(self, contract):
        m = _verified_manifest(contract)
        m["manifest"]["snapshot"]["gold_max_cob_dt"] = "2026-09-05"
        m["manifest"]["snapshot"]["layers_aligned"] = False
        errors, _, _ = gen.evaluate_invariants(m)
        assert any("snapshot_layers_aligned" in e for e in errors)

    def test_readme_drift_is_warning_not_error(self, contract):
        """Static drift KHÔNG được làm collector chết."""
        m = _verified_manifest(contract)
        m["metrics"]["platform"]["data_contracts"]["value"] = 99
        m["metrics"]["platform"]["data_contracts"]["declared"] = 33
        errors, warnings, _ = gen.evaluate_invariants(m)
        assert any("data_contracts" in w for w in warnings)
        assert not any("data_contracts" in e for e in errors)

    def test_dirty_worktree_blocks_promotion(self, contract):
        m = _verified_manifest(contract)
        m["manifest"]["build"]["git_dirty"] = True
        errors, _, _ = gen.evaluate_invariants(m)
        assert any("worktree_clean" in e for e in errors)


# ---------------------------------------------------------------------------
# apply_results / promote
# ---------------------------------------------------------------------------

class TestAssemblyAndPromotion:
    def test_manual_metrics_preserved_not_overwritten(self, contract):
        """
        cdc_freshness là benchmark người đo — generator không được set null.

        So với chính contract chứ không ghim số cụ thể: tính chất cần giữ là
        "đi qua generator mà không mất giá trị", và nó phải đúng sau mỗi lần
        đo lại, không phải chỉ đúng với bộ số của một release.
        """
        expected = contract["metrics"]["cdc_freshness"]
        manual = gen.collect_manual_metrics(contract)
        result = gen.apply_results(contract, {}, {}, manual, {}, "2026-09-06")
        preserved = result["metrics"]["cdc_freshness"]

        assert preserved["trials_seconds"] == expected["trials_seconds"]
        assert preserved["median_seconds"] == expected["median_seconds"]
        assert preserved["median_seconds"] is not None
        # Ngữ cảnh cũng phải sống sót; median không có cadence là số vô nghĩa.
        assert preserved["consolidation_cadence_seconds"] == (
            expected["consolidation_cadence_seconds"]
        )

    def test_layers_aligned_computed_from_layer_values(self, contract):
        runtime = {
            "snapshot.bronze_max_cob_dt": {"value": "2026-09-06"},
            "snapshot.silver_max_cob_dt": {"value": "2026-09-06"},
            "snapshot.gold_max_cob_dt": {"value": "2026-09-06"},
        }
        result = gen.apply_results(contract, {}, runtime, {}, {}, "2026-09-06")
        assert result["manifest"]["snapshot"]["layers_aligned"] is True

    def test_layers_misaligned_when_gold_lags(self, contract):
        runtime = {
            "snapshot.bronze_max_cob_dt": {"value": "2026-09-06"},
            "snapshot.silver_max_cob_dt": {"value": "2026-09-06"},
            "snapshot.gold_max_cob_dt": {"value": "2026-09-05"},
        }
        result = gen.apply_results(contract, {}, runtime, {}, {}, "2026-09-06")
        assert result["manifest"]["snapshot"]["layers_aligned"] is False

    def test_multi_column_query_rows_merge_into_node(self, contract):
        runtime = {
            "silver.snapshot_rows.fact_txn_account": {"rows": 1200000, "distinct_txn_id": 1200000}
        }
        result = gen.apply_results(contract, {}, runtime, {}, {}, "2026-09-06")
        node = result["metrics"]["silver"]["snapshot_rows"]["fact_txn_account"]
        assert node["rows"] == 1200000
        assert node["distinct_txn_id"] == 1200000

    def test_canonical_not_promoted_when_errors(self, contract, tmp_path, monkeypatch):
        """Bad rebuild KHÔNG được tự động trở thành verified evidence."""
        canonical = tmp_path / "metrics-manifest.yaml"
        canonical.write_text("original: true\n", encoding="utf-8")
        monkeypatch.setattr(gen, "MANIFEST_PATH", canonical)

        assert gen.promote_canonical_if_verified(contract, ["boom"]) is False
        assert canonical.read_text(encoding="utf-8") == "original: true\n"

    def test_canonical_promoted_when_clean(self, contract, tmp_path, monkeypatch):
        canonical = tmp_path / "metrics-manifest.yaml"
        canonical.write_text("original: true\n", encoding="utf-8")
        monkeypatch.setattr(gen, "MANIFEST_PATH", canonical)

        assert gen.promote_canonical_if_verified(contract, []) is True
        assert yaml.safe_load(canonical.read_text(encoding="utf-8"))["manifest"]

    def test_run_artifact_written_even_when_failed(self, contract, tmp_path, monkeypatch):
        """
        Rebuild fail vẫn phải để lại forensic evidence — không được mất trắng
        chỉ vì invariant đỏ.
        """
        monkeypatch.setattr(gen, "GENERATED_DIR", tmp_path / "generated")
        failed = yaml.safe_load(yaml.safe_dump(contract))
        failed["manifest"]["runtime"]["generated_at_utc"] = "2026-09-06T02:04:31Z"
        failed["manifest"]["verification"]["status"] = "failed"
        failed["manifest"]["verification"]["errors"] = ["scd2_no_duplicate_current_keys: ..."]

        artifact = gen.write_run_artifact(failed)
        assert artifact.exists()
        written = yaml.safe_load(artifact.read_text(encoding="utf-8"))
        assert written["manifest"]["verification"]["status"] == "failed"
        assert written["manifest"]["verification"]["errors"]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

class TestCli:
    def test_validate_contract_mode_needs_no_cob_dt(self, capsys):
        assert gen.main(["--validate-contract"]) == 0
        assert "hợp lệ" in capsys.readouterr().out

    def test_render_sql_mode_needs_no_trino(self, capsys):
        assert gen.main(["--render-sql", "--cob-dt", "2026-09-06"]) == 0
        out = capsys.readouterr().out
        assert "gold.grain_checks.rfm_segment" in out
        assert "{table}" not in out

    def test_cob_dt_required_for_collection(self):
        with pytest.raises(SystemExit):
            gen.main([])


# ---------------------------------------------------------------------------
# Verification scope — not_collected KHÁC verified
# ---------------------------------------------------------------------------

class TestVerificationScope:
    def test_batch_scope_skips_cdc_metrics(self, contract):
        # Set scope tường minh: canonical manifest sau khi promote mang
        # scope=full, test hợp đồng không được phụ thuộc điều đó.
        m = yaml.safe_load(yaml.safe_dump(contract))
        m["manifest"]["runtime"]["verification_scope"] = "batch"
        skips = gen.scope_skips(m)
        assert skips == ["cdc."]
        assert gen._in_scope("gold.grain_checks.rfm_segment", skips)
        assert not gen._in_scope("cdc.current_state.dim_customer_current", skips)

    def test_full_scope_skips_nothing(self, contract):
        m = yaml.safe_load(yaml.safe_dump(contract))
        m["manifest"]["runtime"]["verification_scope"] = "full"
        assert gen.scope_skips(m) == []

    def test_invalid_scope_raises(self, contract):
        m = yaml.safe_load(yaml.safe_dump(contract))
        m["manifest"]["runtime"]["verification_scope"] = "nonsense"
        with pytest.raises(ValueError, match="verification_scope"):
            gen.scope_skips(m)

    def test_skipped_invariants_are_named_not_silently_passed(self, contract):
        """
        Đây là chốt chặn của cả cơ chế scope: invariant ngoài scope phải xuất
        hiện trong `skipped` với TÊN, không được biến mất như thể đã PASS.
        """
        m = _verified_manifest(contract)
        for node in m["metrics"]["cdc"]["current_state"].values():
            node["duplicate_keys"] = None          # chưa đo
        errors, _, skipped = gen.evaluate_invariants(m, ["cdc."])
        assert not any("cdc_current_state_unique" in e for e in errors)
        assert any("cdc_current_state_unique" in s for s in skipped)

    def test_cdc_invariant_fails_when_in_scope_and_unmeasured(self, contract):
        """Cùng dữ liệu đó, ở scope full thì PHẢI fail."""
        m = _verified_manifest(contract)
        for node in m["metrics"]["cdc"]["current_state"].values():
            node["duplicate_keys"] = None
        errors, _, _ = gen.evaluate_invariants(m, [])
        assert any("cdc_current_state_unique" in e for e in errors)

    def test_batch_queries_exclude_cdc(self, contract):
        m = yaml.safe_load(yaml.safe_dump(contract))
        m["manifest"]["runtime"]["verification_scope"] = "batch"
        skips = gen.scope_skips(m)
        queries = [q for q in gen.render_query_bundle(m, "2026-09-06")
                   if gen._in_scope(q.id, skips)]
        assert not [q for q in queries if q.id.startswith("cdc.")]
        assert [q for q in queries if q.id.startswith("gold.")]


# ---------------------------------------------------------------------------
# Serving visibility — Trino là serving engine
# ---------------------------------------------------------------------------

class TestServingVisibility:
    def test_gold_ddl_declares_no_current_objects(self, contract):
        """
        Sau retirement: 10 CREATE TABLE lich su, 0 CREATE VIEW.
        Gold DDL khong con khai bao bat ky serving object nao.
        """
        assert gen._gold_ddl_objects(contract) == 10
        assert gen._gold_ddl_tables(contract) == 10

        ddl = (PROJECT_ROOT / "docker" / "init_iceberg" / "03_ddl_gold.sql").read_text(
            encoding="utf-8"
        )
        created = re.findall(
            r"CREATE (?:TABLE IF NOT EXISTS|OR REPLACE VIEW)\s+[\w.]*\.(\w+)", ddl
        )
        assert not [c for c in created if c.endswith("_current")], (
            "DDL Gold khong duoc tao lai object *_current — chung thuoc "
            "dbt/models/serving/ va do dbt/Trino so huu"
        )

    def test_legacy_query_uses_information_schema_not_per_table_counts(self):
        """
        Query legacy phai dem qua information_schema, KHONG phai COUNT(*) tren
        tung bang.

        Ban dau no liet ke 8 bang roi COUNT(*) tung cai. Cach do chi chay duoc
        trong giai doan chuyen tiep: sau khi retire, chinh cac bang do bien mat
        nen query nem TABLE_NOT_FOUND — metric "da retire chua" lai chet dung
        luc cau tra loi la "roi". Da xay ra that trong clean rebuild.
        """
        sql = (PROJECT_ROOT / "docs" / "evidence" / "metrics-manifest.sql").read_text(encoding="utf-8")
        block = sql.split("--@id gold.legacy_gold_current_objects")[1].split("--@id")[0]
        assert "information_schema.tables" in block
        assert not re.search(r":catalog\.gold\.\w+_current", block), (
            "khong duoc tham chieu truc tiep bang legacy — chung da bi retire"
        )

    def test_serving_alignment_query_covers_all_nine(self):
        sql = (PROJECT_ROOT / "docs" / "evidence" / "metrics-manifest.sql").read_text(encoding="utf-8")
        block = sql.split("--@id serving.current_snapshot_alignment")[1].split("--@id")[0]
        refs = set(re.findall(r":catalog\.serving\.([a-z0-9_]+)", block))
        assert len(refs) == 9
        assert "mart_customer_360_current" in refs

    def test_serving_contract_invariants_are_blocking(self, contract):
        """Hop dong phuc vu that phai chan; retirement legacy thi khong."""
        for inv_id in ("serving_snapshot_alignment", "serving_objects_complete"):
            assert contract["invariants"][inv_id]["severity"] == "error"
        # Sau retirement, ca hai cung la blocking: khong con ly do de WARN.
        for inv_id in ("legacy_gold_current_retired", "legacy_spark_view_retired"):
            assert contract["invariants"][inv_id]["severity"] == "error"

    def test_spark_view_visibility_is_no_longer_blocking(self, contract):
        """
        DAO CHIEU co chu y: truoc migration, Spark view khong hien thi tren
        Trino la ERROR. Sau migration, nhu cau phuc vu do serving.* dap ung nen
        cau hoi dung la "view da retire chua" — WARN, khong chan.
        """
        m = _verified_manifest(contract)
        errors, _, _ = gen.evaluate_invariants(m)
        assert not any("legacy_spark_view" in e for e in errors)


class TestProvenanceIsNeverFalsified:
    """
    `git_dirty` là một khẳng định về nguồn gốc của chính manifest, nên không cờ
    CLI nào được phép viết lại nó.

    Bug đã có thật trong script này:

        "git_dirty": False if (dirty and allow_dirty) else dirty

    Tác dụng DUY NHẤT của `--allow-dirty` là làm manifest nói dối — trong đúng
    script chịu trách nhiệm promote canonical. Không hề có cổng chặn nào để cờ
    đó nới ra cả. Cờ được phép quyết định CÓ CHẠY hay không; nó không được phép
    quyết định SỰ THẬT là gì.
    """

    @staticmethod
    def _manifest(dirty: bool) -> dict:
        return {"manifest": {"build": {"git_dirty": dirty, "git_commit": "abc1234"}}}

    def test_build_metadata_takes_no_override_flag(self):
        """Không còn tham số nào cho phép bẻ cong provenance."""
        import inspect

        params = inspect.signature(gen.collect_build_metadata).parameters
        assert not params, (
            f"collect_build_metadata nhận tham số {list(params)} — provenance "
            "phải được đo, không phải được truyền vào"
        )

    def test_dirty_tree_blocks_promotion(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gen, "MANIFEST_PATH", tmp_path / "m.yaml")
        assert gen.promote_canonical_if_verified(self._manifest(True), []) is False, (
            "cây bẩn thì số đo không quy được về một commit — không được promote"
        )

    def test_dirty_tree_can_be_overridden_explicitly(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gen, "MANIFEST_PATH", tmp_path / "m.yaml")
        assert gen.promote_canonical_if_verified(
            self._manifest(True), [], allow_dirty=True
        ) is True

    def test_clean_tree_promotes(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gen, "MANIFEST_PATH", tmp_path / "m.yaml")
        assert gen.promote_canonical_if_verified(self._manifest(False), []) is True

    def test_errors_still_block_even_on_a_clean_tree(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gen, "MANIFEST_PATH", tmp_path / "m.yaml")
        assert gen.promote_canonical_if_verified(
            self._manifest(False), ["some blocking error"]
        ) is False
