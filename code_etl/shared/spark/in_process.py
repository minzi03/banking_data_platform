"""
Chạy nhiều job Silver/Gold trong MỘT Spark app — cho bootstrap `--in-process`.

Vì sao: bootstrap mặc định gọi `spark-submit` một lần cho mỗi job. Trên dữ liệu CI (1%),
mỗi lần tốn ~18–20 s gần như toàn bộ là khởi động — JVM, SparkContext, xin executor, nạp
Iceberg — cho vài giây việc thật. 16 job Silver + 14 job Gold = ~9.5 phút trong job CI
"Trino Integration" (đo 2026-09-28, ~67% thời gian của job). Bronze đã chạy 17 bảng trong
một app và mất ~25 s.

Cô lập giữa các job: mỗi job chạy trên `spark.newSession()` — temp view, SQL conf và UDF
riêng, nhưng chung SparkContext/executor nên không khởi động lại. Cache dữ liệu thì dùng
chung giữa các session, nên xoá sau mỗi job (scd_type2 có `.cache()`).

Hàm job được gọi y như `main()` của nó gọi: load_config → validate_config → run_*(spark,
config, cob_dt, logger). Chỉ phần tạo/dừng session là khác; đường `spark-submit` từng job
vẫn là mặc định và vẫn được CI chạy cho mỗi loại job.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

_MODULES: dict[Path, ModuleType] = {}


def load_job_module(path: Path) -> ModuleType:
    """Nạp module job từ file (một lần), dưới tên riêng — `__main__` của nó không chạy."""
    path = path.resolve()
    if path not in _MODULES:
        name = f"_in_process_{path.parent.parent.name}_{path.stem}"
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        _MODULES[path] = module
    return _MODULES[path]


def run_job_in_process(
    spark,
    module: ModuleType,
    runner_name: str,
    config_path: str,
    cob_dt: str,
    logger,
    *,
    check_session: Callable | None = None,
    runner_kwargs: dict | None = None,
) -> bool:
    """Chạy MỘT job trên session con của `spark`. True nếu xong, False nếu job raise."""
    if check_session is None:
        from spark.spark_session import assert_utc_session as check_session
    session = spark.newSession()
    try:
        check_session(session)
        config = module.load_config(config_path)
        module.validate_config(config)
        getattr(module, runner_name)(session, config, cob_dt, logger, **(runner_kwargs or {}))
        return True
    except Exception:  # noqa: BLE001 — bootstrap quyết định dừng hay chạy tiếp, như exit code của subprocess
        logger.exception(f"  ✗ {config_path} FAILED (in-process)")
        return False
    finally:
        spark.catalog.clearCache()
