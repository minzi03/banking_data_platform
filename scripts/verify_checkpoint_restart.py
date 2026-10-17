"""
Checkpoint Restart Smoke Test — QR payment streaming (khoá new2 Buổi 10, P3.1).
==============================================================================

Đề bài (Buổi 10, "Thực hành"):
    *"Kill job → chạy lại từ checkpoint, kiểm tra không trùng/mất."*
Và (Final Project §4.3):
    *"Restart job từ checkpoint không trùng, không mất dữ liệu."*

Đây là bằng chứng CHẠY THẬT cho hai câu đó. Script chạy trên stack Docker đang
hoạt động và tự dựng một "kênh thử nghiệm" RIÊNG để không đụng dữ liệu thật:

    topic:      qr.payment.restart_test
    checkpoint: s3a://lakehouse/checkpoints/cdc/qr_payment_restart_test
    bảng đích:  lakehouse.bronze.qr_restart_test_cdc

## Kịch bản 4 pha

  Pha A  gửi N1 event mới → chạy stream → chờ tới khi Bronze có đủ N1
  Pha B  kill job giữa luồng (SIGKILL spark-submit)          ← "kill job"
  Pha C  gửi N2 event mới + phát lại 1 event của pha A (cùng event-time)
  Pha D  chạy lại stream với CÙNG checkpoint                 ← "restart"

## Điều được chứng minh

  1. RESTART KHÔNG MẤT: cả N1 + N2 transaction_id đều có trong Bronze.
  2. RESTART KHÔNG TRÙNG: watermark/dedup loại event replay trong cửa sổ,
     nên tổng dòng == số transaction_id duy nhất.
  3. CHECKPOINT CÓ TÁC DỤNG: ở pha D, batch đầu tiên tiếp tục đúng từ offset
     đã commit sau pha A, thay vì đọc lại từ offset 0.

Điều KHÔNG được claim: exactly-once ngoài cửa sổ watermark. Bronze là
append-only; dedup chỉ bảo vệ các event có cùng khoá trong watermark state.
Xem docs/02-architecture/architecture.md §"Exactly-once semantics".

## Chạy

    python scripts/verify_checkpoint_restart.py            # full, verbose
    python scripts/verify_checkpoint_restart.py --n1 15 --n2 10
    python scripts/verify_checkpoint_restart.py --json     # in kết quả JSON

Mã thoát: 0 = PASS, 1 = FAIL (kèm lý do), 2 = môi trường không sẵn sàng.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPARK_CONTAINER = "banking-spark-worker-1"
KAFKA_CONTAINER = "banking-kafka"

TOPIC = "qr.payment.restart_test"
CHECKPOINT = "s3a://lakehouse/checkpoints/cdc/qr_payment_restart_test/run"

CONFIG_SRC = "code_etl/cdc/config/cdc_qr_payment.yml"
CONFIG_REL = "code_etl/cdc/config/cdc_qr_payment_restart_test.yml"

TARGET_TABLE = "lakehouse.bronze.qr_restart_test_cdc"
STREAM_APP = "/opt/project/code_etl/cdc/base_job/cdc_streaming.py"

# --- helpers ---------------------------------------------------------------


def log(*parts: object) -> None:
    """Print Unicode logs on Windows consoles with a legacy code page."""
    try:
        print(*parts, flush=True)
    except UnicodeEncodeError:
        stream = getattr(sys, "stdout", None)
        if stream is not None and hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
        print(*parts, flush=True)


def configure_console() -> None:
    """Prefer UTF-8 output so Vietnamese diagnostics do not mask test failures."""
    for stream in (getattr(sys, "stdout", None), getattr(sys, "stderr", None)):
        if stream is not None and hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def run(cmd: list[str], *, check: bool = False, timeout: int = 180) -> subprocess.CompletedProcess:
    # encoding/errors tường minh: console Windows mặc định cp1258 không decode
    # được output UTF-8 từ container (kể cả tên cột tiếng Việt trong log).
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=check,
    )


def docker_exec(container: str, args: list[str], *, timeout: int = 180) -> subprocess.CompletedProcess:
    return run(["docker", "exec", container, *args], timeout=timeout)


def spark_sql(sql: str, *, timeout: int = 300) -> str:
    """Chạy một câu SQL qua spark-sql trong worker, trả stdout thô."""
    res = docker_exec(
        SPARK_CONTAINER,
        [
            "/opt/spark/bin/spark-sql",
            "--master",
            "spark://spark-master:7077",
            "--conf",
            "spark.ui.enabled=false",
            "-e",
            sql,
        ],
        timeout=timeout,
    )
    if res.returncode != 0:
        raise RuntimeError(f"spark-sql lỗi:\n{res.stderr[-2000:]}")
    return res.stdout


def count_rows() -> tuple[int, int]:
    """(tổng dòng, số transaction_id duy nhất) trong bảng đích."""
    sql = f"SELECT count(*) AS total, count(DISTINCT transaction_id) AS uniq FROM {TARGET_TABLE};"
    out = spark_sql(sql)
    # spark-sql in bằng tab. Bỏ dòng header + log.
    for line in out.splitlines():
        parts = line.strip().split("\t")
        if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
            return int(parts[0]), int(parts[1])
    return 0, 0


def table_exists() -> bool:
    out = spark_sql("SHOW TABLES IN lakehouse.bronze LIKE 'qr_restart_test_cdc';")
    return "qr_restart_test_cdc" in out


def topic_offsets() -> dict[int, int]:
    """Tổng offset hiện tại theo partition; topic chưa có → {}."""
    res = docker_exec(
        KAFKA_CONTAINER,
        [
            "kafka-run-class",
            "kafka.tools.GetOffsetShell",
            "--broker-list",
            "localhost:9092",
            "--topic",
            TOPIC,
        ],
    )
    out = res.stdout or ""
    if "does not exist" in (out + (res.stderr or "")):
        return {}
    totals: dict[int, int] = {}
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].isdigit():
            totals[int(parts[1])] = int(parts[2])
    return totals


def produce(txn_ids: list[int] | None, count: int | None, event_ts_ms: int | None) -> None:
    """Gửi event qua producer TRONG container (kafka-python đã có trong image)."""
    args = [
        "python3",
        "/opt/project/scripts/qr_payment_producer.py",
        "--bootstrap",
        "kafka:9092",
        "--topic",
        TOPIC,
        "--rate",
        "50",
    ]
    if txn_ids:
        args += ["--txn-ids", ",".join(str(t) for t in txn_ids)]
    else:
        args += ["--count", str(count), "--seed", "7"]
    if event_ts_ms is not None:
        args += ["--event-ts-ms", str(event_ts_ms)]
    res = docker_exec(SPARK_CONTAINER, ["bash", "-lc", "cd /opt/project && " + " ".join(map(str, args))])
    if res.returncode != 0:
        raise RuntimeError(f"producer lỗi:\n{res.stderr[-1500:]}")
    out_lines = (res.stdout or "").strip().splitlines()
    log(f"    producer: {out_lines[-1] if out_lines else '(không có output)'}")


def start_stream() -> tuple[subprocess.Popen, str]:
    """spark-submit nền; trả (process, log_path)."""
    os.makedirs(os.path.join(REPO_ROOT, "logs"), exist_ok=True)
    log_path = os.path.join(REPO_ROOT, "logs", f"restart_stream_{int(time.time())}.log")
    cmd = [
        "docker",
        "exec",
        SPARK_CONTAINER,
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--conf",
        "spark.driver.memory=1g",
        "--conf",
        "spark.ui.enabled=false",
        "--conf",
        "spark.sql.shuffle.partitions=2",
        STREAM_APP,
        "--config",
        f"/opt/project/{CONFIG_REL}",
        "--kafka_bootstrap",
        "kafka:9092",
    ]
    # File log phải mở không theo context manager kiểu `with`: Popen cần handle
    # sống trong suốt thời gian query chạy, và ta đóng nó sau khi kill.
    fh = open(log_path, "w", encoding="utf-8")  # noqa: SIM115
    proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT, cwd=REPO_ROOT)
    proc._log_fh = fh  # type: ignore[attr-defined]
    return proc, log_path


def kill_stream(proc: subprocess.Popen) -> None:
    """
    Kill CẢ cụm: `docker exec` chỉ giết client, spark-submit bên trong container
    vẫn sống. Phải pkill theo tên config của kênh thử nghiệm.
    """
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except Exception:  # noqa: BLE001
        proc.kill()
    docker_exec(SPARK_CONTAINER, ["pkill", "-9", "-f", "cdc_qr_payment_restart_test"])
    # Đóng handle log sau khi tiến trình đã chết, nếu không sẽ rò file descriptor.
    fh = getattr(proc, "_log_fh", None)
    if fh is not None:
        with contextlib.suppress(OSError):
            fh.close()


def wait_for_rows(expected: int, timeout: float = 180.0) -> int:
    deadline = time.time() + timeout
    last = 0
    while time.time() < deadline:
        last, _ = count_rows()
        if last >= expected:
            return last
        time.sleep(3)
    return last


def first_offsets_in_log(log_path: str) -> dict[str, dict[str, int]] | None:
    """Parse Spark's first reported Kafka offsets after the checkpoint restart."""
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except OSError:
        return None
    patterns = (
        r"Initial offsets: (\{[^\n]*\})",
        # Spark 3.5 logs the restored source position as `Stream started from`
        # rather than `Initial offsets` when a checkpoint already exists.
        rf"Stream started from .*?(\{{\"{re.escape(TOPIC)}\":\{{[^}}]+\}}\}})",
    )
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for raw in reversed(matches):
            try:
                offsets = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(offsets, dict) and TOPIC in offsets:
                return offsets
    return None


def has_resumed_offsets(
    offsets: dict[str, dict[str, int]] | None,
    committed_offsets: dict[int, int],
    current_offsets: dict[int, int],
) -> bool:
    """Require restart offsets at the committed point, before new data."""
    if not offsets:
        return False
    topic_offsets_map = offsets.get(TOPIC)
    if not isinstance(topic_offsets_map, dict) or not topic_offsets_map:
        return False
    for partition, offset in topic_offsets_map.items():
        try:
            partition_id = int(partition)
            offset_value = int(offset)
        except (TypeError, ValueError):
            return False
        committed = committed_offsets.get(partition_id)
        current = current_offsets.get(partition_id)
        if committed is None or current is None or offset_value != committed or offset_value >= current:
            return False
    return True


def has_no_duplicate_keys(total: int, distinct: int) -> bool:
    """Bronze should contain one row per accepted key after stream deduplication."""
    return total == distinct


def replay_event() -> tuple[int, int] | None:
    """Return an accepted event's business ID and event time from test Bronze."""
    output = spark_sql(f"SELECT transaction_id, __cdc_timestamp_ms FROM {TARGET_TABLE} ORDER BY __kafka_offset LIMIT 1")
    for line in output.splitlines():
        parts = line.strip().split("\t")
        if len(parts) == 2 and all(part.strip().isdigit() for part in parts):
            return int(parts[0]), int(parts[1])
    return None


def wait_for_stream_start(log_path: str, timeout: float = 60.0) -> bool:
    """Wait until Spark reports a started query or a terminal process failure."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with open(log_path, encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except OSError:
            return False
        if "Streaming query started:" in text:
            return True
        time.sleep(1)
    return False


# --- main ------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    configure_console()
    ap = argparse.ArgumentParser(description="Checkpoint restart smoke test (QR streaming)")
    ap.add_argument("--n1", type=int, default=12, help="Số event pha A")
    ap.add_argument("--n2", type=int, default=8, help="Số event mới pha C")
    ap.add_argument("--json", action="store_true", help="In kết quả JSON")
    ap.add_argument("--keep", action="store_true", help="Giữ checkpoint/topic/bảng sau khi chạy")
    args = ap.parse_args(argv)

    if args.n1 < 2 or args.n2 < 1:
        ap.error("--n1 phải >= 2 và --n2 phải >= 1")

    result: dict = {
        "test": "checkpoint_restart_qr_streaming",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "tree": "PASS",
        "checks": {},
    }

    def finish(code: int) -> int:
        result["exit_code"] = code
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return code

    test_config_path = os.path.join(REPO_ROOT, CONFIG_REL)
    proc: subprocess.Popen | None = None
    log_path: str | None = None
    log_paths: list[str] = []
    table_created = False
    config_created = False

    try:
        # --- 0. Environment ------------------------------------------------
        log("[0] Kiểm tra môi trường")
        for container in (SPARK_CONTAINER, KAFKA_CONTAINER):
            if docker_exec(container, ["true"]).returncode != 0:
                log(f"    ✗ container {container} không chạy — cần `make up`")
                result["tree"] = "ENV"
                return finish(2)
        log("    ✓ spark-worker + kafka đang chạy")

        if table_exists():
            log(f"    ✗ bảng riêng {TARGET_TABLE} đã tồn tại từ lần chạy trước; dọn bảng test trước khi chạy lại")
            result["tree"] = "ENV"
            return finish(2)
        log("[0b] Bảng test chưa tồn tại")

        if topic_offsets():
            log(f"    ✗ topic riêng {TOPIC} đã có dữ liệu; không xoá topic không do test tạo")
            result["tree"] = "ENV"
            return finish(2)

        with open(os.path.join(REPO_ROOT, CONFIG_SRC), encoding="utf-8") as src:
            cfg_text = src.read()
        cfg_text = cfg_text.replace("topic: qr.payment.events", f"topic: {TOPIC}")
        cfg_text = cfg_text.replace(
            "checkpoint_location: s3a://lakehouse/checkpoints/cdc/qr_payment_v1",
            f"checkpoint_location: {CHECKPOINT}",
        )
        cfg_text = cfg_text.replace("table: qr_payment_cdc", "table: qr_restart_test_cdc")
        if TOPIC not in cfg_text or CHECKPOINT not in cfg_text:
            raise RuntimeError(
                "Không tạo được config test: cdc_qr_payment.yml đã đổi shape "
                "(topic/checkpoint không còn khớp chuỗi để thay)."
            )
        config_created = True
        with open(test_config_path, "x", encoding="utf-8") as f:
            f.write("# AUTO-GENERATED bởi scripts/verify_checkpoint_restart.py — kênh thử nghiệm\n" + cfg_text)
        log(f"    ✓ config thử nghiệm: {CONFIG_REL}")

        spark_sql(
            f"""CREATE TABLE {TARGET_TABLE} (
                transaction_id BIGINT, account_id BIGINT, device_id BIGINT, location_id BIGINT,
                customer_id BIGINT, transaction_type VARCHAR(50), channel VARCHAR(50),
                amount DECIMAL(18,2), currency VARCHAR(10), is_fraud VARCHAR(10),
                fraud_reason VARCHAR(500), status VARCHAR(20), merchant_id BIGINT,
                payment_channel VARCHAR(10), refund_flag INT, transaction_date BIGINT,
                created_ts BIGINT, last_updated BIGINT, __cdc_operation VARCHAR(10),
                __cdc_timestamp TIMESTAMP, __cdc_timestamp_ms BIGINT, __spark_batch_id BIGINT,
                __ingestion_time TIMESTAMP, __kafka_partition INT, __kafka_offset BIGINT
            ) USING iceberg PARTITIONED BY (DATE(__cdc_timestamp))"""
        )
        table_created = True
        log(f"    ✓ bảng đích: {TARGET_TABLE}")

        # Generate disjoint IDs so only the explicit replay can produce duplicates.
        base_id = int(time.time() * 1000) % 1_000_000_000
        phase_a_ids = list(range(base_id, base_id + args.n1))
        phase_c_ids = list(range(base_id + args.n1, base_id + args.n1 + args.n2))
        event_ts_ms = int(time.time() * 1000)

        # Phase A — start from offset 0 and process the first unique batch.
        log(f"\n[A] Gửi {args.n1} event mới + chạy stream")
        produce(phase_a_ids, None, event_ts_ms=event_ts_ms)
        offsets_a = topic_offsets()
        proc, log_path = start_stream()
        log_paths.append(log_path)
        if not wait_for_stream_start(log_path):
            raise RuntimeError(f"stream pha A không khởi động; xem {log_path}")
        got_a = wait_for_rows(args.n1)
        if got_a < args.n1:
            raise RuntimeError(f"pha A timeout: Bronze có {got_a}/{args.n1} dòng; xem {log_path}")
        result["phase_a"] = {"sent": args.n1, "rows": got_a, "offsets": offsets_a}
        result["checks"]["phase_a_rows"] = True

        # Phase B — kill the driver and its Spark query.
        log("\n[B] Kill streaming job (SIGKILL)")
        kill_stream(proc)
        proc = None
        time.sleep(2)
        rows_b, _ = count_rows()
        result["phase_b"] = {"rows_after_kill": rows_b}

        # Phase C — append new IDs and deliberately replay one ID with original event time.
        log(f"\n[C] Gửi {args.n2} event mới + phát lại 1 event cũ")
        replay = replay_event()
        if replay is None:
            raise RuntimeError("không tìm thấy event hợp lệ để replay")
        replay_id, replay_ts = replay
        produce(phase_c_ids, None, event_ts_ms=event_ts_ms)
        produce([replay_id], None, event_ts_ms=replay_ts)
        offsets_before_restart = topic_offsets()
        result["phase_c"] = {"new": args.n2, "replayed": replay_id, "offsets": offsets_before_restart}

        # Phase D — use the exact same checkpoint. The replay has the original
        # event time and must be removed by the watermark/dedup state.
        log("\n[D] Restart stream với cùng checkpoint")
        proc, log_path = start_stream()
        log_paths.append(log_path)
        if not wait_for_stream_start(log_path):
            raise RuntimeError(f"stream pha D không khởi động; xem {log_path}")
        expected_unique = args.n1 + args.n2
        deadline = time.time() + 180
        total_d, uniq_d = 0, 0
        while time.time() < deadline:
            total_d, uniq_d = count_rows()
            if uniq_d >= expected_unique:
                break
            time.sleep(3)
        time.sleep(3)
        total_d, uniq_d = count_rows()
        offsets_d = first_offsets_in_log(log_path)
        kill_stream(proc)
        proc = None

        checks = {
            "no_loss": uniq_d == expected_unique,
            "no_duplicate_keys": has_no_duplicate_keys(total_d, uniq_d),
            "checkpoint_resumed": has_resumed_offsets(offsets_d, offsets_a, offsets_before_restart),
        }
        result["checks"].update(checks)
        result["phase_d"] = {
            "rows": total_d,
            "distinct": uniq_d,
            "expected_unique": expected_unique,
            "first_offsets_after_restart": offsets_d,
            "topic_offsets_before_restart": offsets_before_restart,
        }
        result["stream_logs"] = {"phase_a": log_paths[0], "phase_d": log_paths[1]}

        for name, ok in checks.items():
            log(f"  {'✓' if ok else '✗'} {name}")
        result["tree"] = "PASS" if all(checks.values()) else "FAIL"
        log("KẾT QUẢ:", result["tree"])
        return finish(0 if result["tree"] == "PASS" else 1)
    except Exception as exc:  # noqa: BLE001
        result["tree"] = "FAIL"
        result["error"] = str(exc)
        log(f"✗ {exc}")
        return finish(1)
    finally:
        if proc is not None:
            try:
                kill_stream(proc)
            except Exception as exc:  # noqa: BLE001
                log(f"⚠ không dừng được test stream: {exc}")
        if not args.keep:
            try:
                if table_created:
                    spark_sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
            except Exception as exc:  # noqa: BLE001
                log(f"⚠ cleanup bảng lỗi: {exc}")
            if config_created and os.path.exists(test_config_path):
                try:
                    os.remove(test_config_path)
                except OSError as exc:
                    log(f"⚠ cleanup config lỗi: {exc}")

        # The isolated test topic and checkpoint are deliberately never deleted
        # here: removing them could touch a shared resource. A rerun reports ENV
        # until they are cleaned, which is recorded in the evidence README.


if __name__ == "__main__":
    sys.exit(main())
