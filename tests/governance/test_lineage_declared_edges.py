"""
Contract test — lineage lấy từ khai báo của job, không từ danh sách viết tay
==========================================================================

`governance.lineage.declared_edges` đọc `source.tables` / `target` / `job.type`
trong YAML của job Silver/Gold. ops_lineage_dag ghi đúng các cạnh đó vào
`opslakehouse.lineage_log`.

Trước đây DAG giữ danh sách cạnh viết tay: 3/11 cạnh vào Gold không tồn tại,
43/51 cạnh thật bị thiếu, 9/14 bảng Gold vắng mặt (TD-13).

Chạy: pytest tests/governance/test_lineage_declared_edges.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from governance.lineage import EDGE_LAYERS, TransformType, declared_edges

REPO_ROOT = Path(__file__).resolve().parents[2]
ETL_ROOT = REPO_ROOT / "code_etl"
LINEAGE_DAG = REPO_ROOT / "airflow" / "dags" / "ops" / "ops_lineage_dag.py"


def _job_configs() -> list[dict]:
    configs = []
    for layer in EDGE_LAYERS:
        for path in sorted((ETL_ROOT / layer).rglob("*.yml")):
            config = yaml.safe_load(path.read_text(encoding="utf-8"))
            if isinstance(config, dict) and isinstance(config.get("sql"), str):
                configs.append(config)
    return configs


@pytest.fixture(scope="module")
def edges() -> list[tuple[str, str, str]]:
    return declared_edges(ETL_ROOT)


def test_every_job_is_a_target(edges):
    """Mỗi job Silver/Gold có ít nhất một cạnh — không bảng nào vắng khỏi lineage."""
    targets = {tgt for _src, tgt, _t in edges}
    expected = {f"lakehouse.{c['target']['schema']}.{c['target']['table']}" for c in _job_configs()}
    assert len(expected) >= 30, "glob hỏng? quá ít job config"
    assert targets == expected


def test_edges_are_exactly_the_declared_sources(edges):
    """Không thêm, không bớt: mỗi `source.tables` là đúng một cạnh."""
    expected = sorted(
        (f"lakehouse.{src}", f"lakehouse.{c['target']['schema']}.{c['target']['table']}")
        for c in _job_configs()
        for src in c["source"]["tables"]
    )
    assert sorted((s, t) for s, t, _ in edges) == expected
    assert len(edges) == len(set(edges)), "cạnh trùng"


def test_gold_edges_include_the_ones_the_old_list_missed(edges):
    """Hai cạnh cụ thể mà danh sách viết tay sai: một thiếu, một bịa."""
    pairs = {(s, t) for s, t, _ in edges}
    assert ("lakehouse.silver.fact_crm_interaction", "lakehouse.gold.mart_customer_360") in pairs
    # customer_360 không đọc fact_loan_payment — CTE đọc nó là code chết, đã gỡ
    assert ("lakehouse.silver.fact_loan_payment", "lakehouse.gold.mart_customer_360") not in pairs
    assert ("lakehouse.gold.mart_customer_360", "lakehouse.gold.rfm_segment") not in pairs


def test_transform_types(edges):
    by_target = {t: kind for _s, t, kind in edges}
    assert by_target["lakehouse.silver.dim_customer"] == TransformType.SCD2_MERGE
    assert by_target["lakehouse.silver.dim_branch"] == TransformType.SCD1_UPSERT
    assert by_target["lakehouse.silver.fact_txn_account"] == TransformType.FACT_LOAD
    assert all(kind == TransformType.GOLD_MART for _s, t, kind in edges if ".gold." in t)


def test_unknown_job_type_is_rejected(tmp_path):
    job = tmp_path / "gold" / "new" / "job.yml"
    job.parent.mkdir(parents=True)
    job.write_text(
        yaml.safe_dump(
            {
                "job": {"type": "brand_new_kind"},
                "source": {"tables": ["silver.dim_customer"]},
                "target": {"catalog": "lakehouse", "schema": "gold", "table": "x"},
                "sql": "SELECT 1 FROM dim_customer",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="brand_new_kind"):
        declared_edges(tmp_path)


def test_dag_has_no_hand_written_edges():
    """DAG không được khai lại một bảng nào bằng tay — nguồn duy nhất là declared_edges."""
    source = LINEAGE_DAG.read_text(encoding="utf-8")
    assert "declared_edges" in source
    assert "lakehouse.silver." not in source and "lakehouse.gold." not in source
