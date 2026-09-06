"""
Contract cho các bootstrap orchestrator (Silver, Gold).

Silver và Gold bootstrap không tự chạy Spark — chúng gọi `spark-submit` cho
từng job qua `subprocess.run`. Nghĩa là tên chương trình đó phải phân giải được
trong container, và cả hai phải nhất quán.

Lỗi đã đo được trên CI: Gold để `default="spark-submit"` (tên trần, phụ thuộc
PATH) trong khi Silver dùng `/opt/spark/bin/spark-submit`. Vào container bằng
`docker exec`, PATH không có /opt/spark/bin, nên Silver chạy 13/13 job còn Gold
chết `[Errno 2] No such file or directory: 'spark-submit'` với 0/10 job. Một
dòng khác nhau giữa hai file anh em, không ai bắt được vì không có gì so sánh
chúng.

Phân tích TĨNH bằng `ast`: không import module (chúng kéo theo pyspark), không
cần Spark, chạy trong job unit của CI.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

BOOTSTRAPS = {
    "silver": REPO_ROOT / "code_etl" / "silver" / "bootstrap" / "initial_load.py",
    "gold": REPO_ROOT / "code_etl" / "gold" / "bootstrap" / "initial_load.py",
}


def _argument_default(source_path: Path, flag: str) -> str | None:
    """
    Tìm `parser.add_argument("<flag>", default=...)` và trả về hằng default.

    Trả None khi không thấy flag; trả chuỗi rỗng khi có flag nhưng default không
    phải hằng chuỗi — hai trường hợp này test bên dưới xử lý riêng, để lỗi nói
    đúng chuyện gì xảy ra thay vì chỉ báo "không khớp".
    """
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "add_argument"):
            continue
        if not node.args:
            continue
        first = node.args[0]
        if not (isinstance(first, ast.Constant) and first.value == flag):
            continue
        for keyword in node.keywords:
            if keyword.arg == "default":
                if isinstance(keyword.value, ast.Constant) and isinstance(
                    keyword.value.value, str
                ):
                    return keyword.value.value
                return ""
    return None


@pytest.mark.parametrize("layer", sorted(BOOTSTRAPS))
def test_spark_submit_default_is_an_absolute_path(layer):
    """
    Tên trần phụ thuộc PATH của tiến trình con; đường dẫn tuyệt đối thì không.
    Đây là điều kiện để bootstrap chạy được qua `docker exec`.
    """
    default = _argument_default(BOOTSTRAPS[layer], "--spark_submit")

    assert default is not None, (
        f"{layer} bootstrap không còn tham số --spark_submit — "
        "nếu đã đổi cách gọi spark-submit thì cập nhật contract này"
    )
    assert default != "", f"{layer} bootstrap: default của --spark_submit không phải hằng chuỗi"
    assert default.startswith("/"), (
        f"{layer} bootstrap dùng default {default!r}. Tên trần phân giải theo PATH, "
        "mà /opt/spark/bin không nằm trong PATH khi vào container bằng docker exec — "
        "đã đo: Gold chạy 0/10 job với [Errno 2] No such file or directory"
    )


def test_bootstrap_layers_agree_on_spark_submit():
    """
    Hai file anh em phải cùng một giá trị. Test ở trên đã chặn tên trần; test
    này chặn kiểu trôi tinh vi hơn — hai đường dẫn tuyệt đối nhưng khác nhau.
    """
    defaults = {
        layer: _argument_default(path, "--spark_submit")
        for layer, path in BOOTSTRAPS.items()
    }
    assert len(set(defaults.values())) == 1, (
        f"bootstrap các layer bất đồng về spark-submit: {defaults}"
    )
