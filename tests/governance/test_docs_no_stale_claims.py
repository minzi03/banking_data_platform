"""
Chống stale claim quay lại trong tài liệu.

README, ARCHITECTURE.md và docs/architecture/architecture.md phải mô tả CÙNG một
hệ thống. Nếu README nói `10 historical Gold + 9 dbt serving + UTC/ICT explicit`
còn architecture docs vẫn nói `18 Gold + ephemeral semantic layer + Spark ICT`,
thì repo tự mâu thuẫn ngay trước mắt reviewer.

Đây KHÔNG phải markdown parser — chỉ là exact stale-claim regression. Mỗi mục
dưới đây là một claim đã được chứng minh sai bằng runtime evidence, kèm lý do,
để người sau biết vì sao nó bị cấm chứ không chỉ thấy một danh sách chuỗi.

Chạy trong CI, không cần stack.
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

DOCS = [
    REPO_ROOT / "README.md",
    REPO_ROOT / "ARCHITECTURE.md",
    REPO_ROOT / "docs" / "architecture" / "architecture.md",
]

# (chuỗi bị cấm, lý do)
BANNED_CLAIMS = [
    ("18 Gold analytics tables",
     "8 CTAS *_current đã retire; còn 10 historical Gold + 9 dbt serving"),
    ("18 Analytics Tables",
     "cùng lý do — đếm cũ gộp cả CTAS đã retire"),
    ("12 dbt models",
     "12 model sm_* là passthrough ephemeral và đã bị gỡ; còn 9 serving model"),
    ("4.6M+ curated financial transaction records",
     "đếm COUNT(*) cộng dồn nhiều cob_dt partition; số đúng là 2.300.000 distinct"),
    ("per-partition persisted progress tracking",
     "watermark KHÔNG partition-aware — chỉ (timestamp, batch id) theo bảng"),
    ("persisted per-partition watermarks",
     "cùng lý do"),
    ("using per-partition watermarks",
     "cùng lý do"),
    ("spark.sql.session.timeZone = Asia/Ho_Chi_Minh",
     "session timezone là UTC; ICT chỉ dùng để derive business date tường minh"),
    ("freshness below one minute",
     "đo lại trên v1.1 với cadence */10: median 409.8s. Số <1 phút cũ đo khi chạy "
     "consolidation bằng tay, không gồm thời gian chờ lịch"),
    ("completed in under one minute",
     "cùng lý do — không trial nào của v1.1 dưới 1 phút"),
    ("| Median local E2E               |          49.8s |",
     "bảng tổng hợp trong architecture.md phải theo số đã đo lại"),
    # --- Text BÊN TRONG sơ đồ mermaid ---
    # Vòng đầu chỉ chặn câu văn xuôi, nên mermaid vẫn giữ nguyên kiến trúc cũ
    # (18 Tables / 12 Gold Models / 312 Tests / Per-Partition Watermarks) trong
    # khi phần chữ quanh nó đã nói khác. Node label là thứ reviewer nhìn TRƯỚC,
    # nên nó phải nằm trong cùng một hợp đồng.
    ("Gold Analytics<br/>18 Tables",
     "node mermaid vẫn đếm cả 8 bảng CTAS đã retire"),
    ("dbt<br/>12 Gold Models",
     "dbt không còn 12 semantic model; nó publish 9 serving model"),
    ("12 Gold Semantic Models",
     "cùng lý do"),
    ("312 Automated Tests",
     "số test trong sơ đồ phải khớp manifest, không đóng băng ở v1.0"),
    ("Per-Partition Watermarks",
     "watermark là (timestamp, batch id) theo bảng — sơ đồ nói sai giống prose cũ"),
]

# Claim chỉ bị cấm trong ngữ cảnh mô tả Trino (Spark vẫn gọi warehouse là lakehouse)
TRINO_CATALOG_HINTS = ("trino", "Trino")


def _docs_with(text: str) -> list[str]:
    hits = []
    for path in DOCS:
        if not path.exists():
            continue
        if text in path.read_text(encoding="utf-8"):
            hits.append(path.name)
    return hits


@pytest.mark.parametrize(("claim", "reason"), BANNED_CLAIMS, ids=[c[:40] for c, _ in BANNED_CLAIMS])
def test_stale_claim_absent(claim, reason):
    hits = _docs_with(claim)
    assert not hits, f"claim đã lỗi thời còn trong {hits}: {claim!r}\n  lý do: {reason}"


class TestDocsAgreeOnArchitecture:
    """Ba tài liệu phải nói cùng một kiến trúc, không chỉ tránh claim cũ."""

    def test_all_docs_state_serving_ownership(self):
        """
        Ownership boundary là thay đổi kiến trúc lớn nhất của v1.1 — mọi tài
        liệu kiến trúc phải nêu, nếu không reviewer đọc file khác sẽ hiểu sai
        ai sở hữu tầng phục vụ.
        """
        missing = [
            p.name for p in DOCS
            if p.exists() and "serving" not in p.read_text(encoding="utf-8").lower()
        ]
        assert not missing, f"tài liệu không nhắc tầng serving: {missing}"

    def test_all_docs_state_time_semantics(self):
        missing = [
            p.name for p in DOCS
            if p.exists() and "from_utc_timestamp" not in p.read_text(encoding="utf-8")
        ]
        assert not missing, (
            f"tài liệu không nêu cách derive business date: {missing} — "
            "timezone semantics là design decision, không được biến mất sau khi fix"
        )

    def test_architecture_docs_declare_what_is_not_implemented(self):
        """
        Tài liệu kiến trúc phải nói rõ cái CHƯA làm. Đây là thứ giữ cho watermark
        claim không tự leo thang trở lại.
        """
        for path in DOCS[1:]:  # ARCHITECTURE.md + docs/architecture
            text = path.read_text(encoding="utf-8")
            assert "not partition-aware" in text.lower() or "**not** partition-aware" in text, (
                f"{path.name}: phải nêu rõ watermark chưa partition-aware"
            )

    def test_freshness_number_matches_the_manifest_everywhere(self):
        """
        Số freshness là manual metric — generator không sinh lại được, nên nó
        rất dễ trôi. Bất kỳ tài liệu nào nêu freshness phải nêu đúng median
        trong manifest VÀ nêu cadence, vì thiếu cadence thì con số vô nghĩa.
        """
        import yaml

        manifest = yaml.safe_load(
            (REPO_ROOT / "docs" / "evidence" / "metrics-manifest.yaml").read_text(
                encoding="utf-8"
            )
        )
        freshness = manifest["metrics"]["cdc_freshness"]
        median = str(freshness["median_seconds"])
        cadence = str(freshness["consolidation_cadence_seconds"])

        for path in DOCS:
            text = path.read_text(encoding="utf-8")
            if "freshness" not in text.lower():
                continue
            assert median in text, f"{path.name}: thiếu median đã đo ({median}s)"
            assert cadence in text, (
                f"{path.name}: nêu freshness mà không nêu cadence ({cadence}s) — "
                "con số chỉ có nghĩa kèm cadence sinh ra nó"
            )

    def test_mermaid_serving_node_exists_where_a_diagram_exists(self):
        """
        Ownership boundary phải nhìn thấy được TRONG sơ đồ, không chỉ trong
        đoạn văn bên dưới. Prose từng được sync còn mermaid thì không, và sơ đồ
        là thứ reviewer nhìn trước.
        """
        for path in DOCS:
            text = path.read_text(encoding="utf-8")
            if "```mermaid" not in text:
                continue
            assert "iceberg.serving" in text, (
                f"{path.name}: sơ đồ không có node tầng serving do dbt sở hữu"
            )

    def test_test_count_in_docs_matches_manifest(self):
        """
        Số test xuất hiện ở cả bảng metric lẫn node sơ đồ. Neo cả hai vào
        manifest để không có chỗ nào đóng băng lại ở con số cũ.
        """
        import yaml

        manifest = yaml.safe_load(
            (REPO_ROOT / "docs" / "evidence" / "metrics-manifest.yaml").read_text(
                encoding="utf-8"
            )
        )
        expected = str(
            manifest["metrics"]["platform"]["automated_tests"]["test_functions"]["value"]
        )
        for path in DOCS:
            text = path.read_text(encoding="utf-8")
            if "Automated Tests" not in text and "Automated tests" not in text:
                continue
            assert expected in text, (
                f"{path.name}: nêu số test nhưng không phải {expected} (giá trị trong manifest)"
            )

    def test_gold_counts_consistent_across_docs(self):
        """10 historical + 9 serving phải xuất hiện nhất quán."""
        for path in DOCS:
            text = path.read_text(encoding="utf-8")
            assert "10 historical Gold" in text or "Historical Gold" in text, (
                f"{path.name}: thiếu số historical Gold table"
            )
