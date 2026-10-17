"""
QR Payment Producer — giả lập sự kiện thanh toán QR (khoá new2 §3.1, Buổi 9).
=============================================================================

Đề bài: *"QR Payment Producer: Push events directly to Kafka"* và
*"Producer Python giả lập sự kiện giao dịch"*.

Producer này bơm sự kiện thanh toán QR vào Kafka, để luồng Phase 2 có nguồn
liên tục mà không cần bật Debezium cho một bảng riêng.

## Vì sao phát ĐÚNG envelope Debezium

Consumer hiện tại (`cdc_streaming.py` → `cdc_dlq.validate_and_split`) đọc
Kafka record theo format Debezium:

    {"payload": {"transaction_id": ..., "__op": "c", "__ts_ms": "..."}}

Producer phát **cùng shape** đó thay vì JSON phẳng. Nhờ vậy cùng một streaming
job, cùng DLQ, cùng watermark/dedup áp dụng cho cả 2 nguồn — không phải viết
job thứ hai. Đây chính là ý đồ của Debezium: CDC và event đều là "log thay đổi".

## Topic

`qr.payment.events` — tách khỏi topic Debezium (`postgresql.banking.*`) vì nó
KHÔNG đến từ replication slot. Cấu hình ở `code_etl/cdc/config/cdc_qr_payment.yml`.

## Cách chạy

    # Trên host, trỏ tới Kafka đã map cổng 9092
    pip install kafka-python
    python scripts/qr_payment_producer.py --bootstrap localhost:9092 --rate 5 --count 100

    # Trong mạng docker
    docker exec banking-spark-worker-1 python3 /opt/project/scripts/qr_payment_producer.py \
        --bootstrap kafka:9092 --rate 10

    # Khô (không gửi) — in ra màn hình để kiểm tra payload
    python scripts/qr_payment_producer.py --dry-run --count 5
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone

DEFAULT_TOPIC = "qr.payment.events"
DEFAULT_CURRENCIES = ["VND"]

# Trọng số trạng thái: đa số giao dịch QR thành công, có một ít lỗi/chờ để
# dashboard và tỷ lệ lỗi theo phút có dữ liệu thật.
STATUS_WEIGHTS = [("SUCCESS", 0.90), ("FAILED", 0.06), ("PENDING", 0.04)]


def _pick_status(rng: random.Random) -> str:
    r = rng.random()
    acc = 0.0
    for status, weight in STATUS_WEIGHTS:
        acc += weight
        if r <= acc:
            return status
    return STATUS_WEIGHTS[-1][0]


def build_qr_event(
    transaction_id: int,
    merchant_id: int,
    *,
    now: datetime | None = None,
    rng: random.Random | None = None,
    operation: str = "c",
    event_ts_ms: int | None = None,
) -> dict:
    """
    Dựng một event QR đúng envelope Debezium.

    Trả về dict `{"payload": {...}}` — chính là body Kafka; key của record là
    `transaction_id` để mọi event của cùng giao dịch vào cùng partition
    (điều kiện để dedup theo khoá có ý nghĩa).
    """
    rng = rng or random.Random()
    now = now or datetime.now(timezone.utc)
    # `event_ts_ms` cho phép tiêm event mang event-time CỐ ĐỊNH — dùng trong
    # checkpoint-restart test để phát lại đúng một event cũ. Không có nó, event
    # replay mang timestamp mới và sẽ không rơi vào cửa sổ watermark cũ.
    ts_ms = str(event_ts_ms if event_ts_ms is not None else int(now.timestamp() * 1000))

    amount = round(rng.uniform(20_000, 5_000_000), 2)
    status = _pick_status(rng)

    payload = {
        # `__op` / `__ts_ms` là metadata Debezium mà cdc_dlq đọc.
        "__op": operation,
        "__ts_ms": ts_ms,
        "__deleted": "true" if operation == "d" else "false",
        "transaction_id": transaction_id,
        "account_id": rng.randint(1, 30_000),
        "device_id": rng.randint(1, 500),
        "location_id": rng.randint(1, 50),
        "customer_id": rng.randint(1, 10_000),
        "transaction_type": "PAYMENT",
        "channel": "MOBILE",
        "amount": f"{amount:.2f}",  # DECIMAL dạng chuỗi — đúng decimal.handling.mode=string
        "currency": DEFAULT_CURRENCIES[0],
        "is_fraud": "0",
        "fraud_reason": None,
        # Merchant QR/POS (khoá new2)
        "merchant_id": merchant_id,
        "payment_channel": "QR",
        "refund_flag": "0",
        "status": status,
        "transaction_date": ts_ms,
        "created_ts": ts_ms,
        "last_updated": ts_ms,
    }
    return {"payload": payload}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="QR payment event producer (mock)")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--rate", type=float, default=5.0, help="Số event/giây")
    parser.add_argument("--count", type=int, default=0, help="Dừng sau N event (0 = chạy mãi)")
    parser.add_argument("--merchant-ids", default="1-20", help="Dải merchant, vd 1-20")
    parser.add_argument("--seed", type=int, default=None, help="Seed để tái lập")
    parser.add_argument("--dry-run", action="store_true", help="In payload, không gửi Kafka")
    parser.add_argument(
        "--txn-ids",
        default=None,
        help="Danh sách transaction_id cố định, phân tách bằng dấu phẩy (dùng để phát lại event cũ)",
    )
    parser.add_argument(
        "--event-ts-ms",
        type=int,
        default=None,
        help="Event time cố định (ms) cho mọi event — dùng khi phát lại để rơi đúng cửa sổ watermark",
    )
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)

    try:
        lo, hi = (int(x) for x in args.merchant_ids.split("-"))
    except ValueError:
        parser.error(f"--merchant-ids phải có dạng 'lo-hi', nhận: {args.merchant_ids!r}")

    fixed_txn_ids: list[int] = []
    if args.txn_ids:
        try:
            fixed_txn_ids = [int(x) for x in args.txn_ids.split(",") if x.strip()]
        except ValueError:
            parser.error(f"--txn-ids phải là danh sách số nguyên: {args.txn_ids!r}")
        if not fixed_txn_ids:
            parser.error("--txn-ids rỗng")
        # Gửi đúng danh sách **một lần**, không lặp vô hạn.
        args.count = len(fixed_txn_ids)

    producer = None
    if not args.dry_run:
        try:
            from kafka import KafkaProducer
        except ImportError:
            print(
                "Thiếu thư viện kafka-python. Cài: pip install kafka-python (hoặc chạy với --dry-run để xem payload).",
                file=sys.stderr,
            )
            return 2
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks="all",  # at-least-once; dedup ở stream xử lý phần còn lại
            retries=3,
            linger_ms=10,
        )

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    sent = 0
    started = time.time()
    print(f"QR producer -> {args.topic} @ {args.bootstrap} ({args.rate}/s, dry_run={args.dry_run})")

    try:
        while True:
            transaction_id = fixed_txn_ids[sent] if fixed_txn_ids else rng.randint(10_000_000, 99_999_999)
            merchant_id = rng.randint(lo, hi)
            event = build_qr_event(transaction_id, merchant_id, rng=rng, event_ts_ms=args.event_ts_ms)

            if producer is None:
                print(json.dumps(event, ensure_ascii=False))
            else:
                producer.send(args.topic, key=transaction_id, value=event)

            sent += 1
            if args.count and sent >= args.count:
                break
            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\nDừng theo yêu cầu.")
    finally:
        if producer is not None:
            producer.flush(timeout=10)
            producer.close(timeout=10)

    elapsed = time.time() - started
    print(f"Đã gửi {sent} event trong {elapsed:.1f}s ({sent / elapsed if elapsed else 0:.1f}/s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
