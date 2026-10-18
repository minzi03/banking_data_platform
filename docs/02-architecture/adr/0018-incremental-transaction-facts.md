# ADR-0018 — Ba fact giao dịch nạp tăng dần theo ngày nghiệp vụ

**Status**: Accepted — 2026-09-30
**Liên quan**: [`0004`](0004-business-date-under-utc-session.md) · [`0007`](0007-overwrite-partitions-by-cob-dt.md) · [`0017`](0017-single-cob-dt-definition.md) · benchmark [`scale-x10-2026-09-30`](../../evidence/benchmarks/scale-x10-2026-09-30/README.md)

---

## Context

Mọi bảng Bronze nạp `full_snapshot`: mỗi `cob_dt` đọc lại toàn bộ bảng nguồn, Silver ghi lại
toàn bộ. Với dimension (vài chục nghìn dòng) điều đó rẻ và cho lịch sử SCD2 đơn giản. Với ba
bảng giao dịch — `txn_account`, `card_txn`, `online_transaction`, 23 M trên 26,7 M dòng nguồn ở
×10 — thì không: benchmark ×10 đọc/ghi lại 23 M giao dịch mỗi ngày dù một ngày chỉ phát sinh vài
chục nghìn, và dung lượng lịch sử tăng theo *số ngày × toàn bộ lịch sử*.

## Decision

1. **Phạm vi**: ba bảng giao dịch ở Bronze và Silver. `loan_payment` (lịch trả nợ, trạng thái đổi
   theo thời gian), CRM, support ticket và mọi dimension giữ `full_snapshot`.
2. **Nghĩa của `cob_dt`** với ba bảng này: **ngày nghiệp vụ** (giờ Asia/Ho_Chi_Minh) của giao
   dịch. Mỗi giao dịch nằm đúng một partition. Bronze tính trong SQL nguồn:
   `(timezone('Asia/Ho_Chi_Minh', timezone('UTC', txn_date)))::date AS cob_dt`
   (cột lưu UTC — ADR-0004).
3. **Hằng ngày** (`cob_dt = D`): Bronze lấy `txn_date` trong `[D 00:00 ICT, D+1 00:00 ICT)` đổi
   sang UTC; Silver đọc đúng partition D. `overwritePartitions` thay đúng partition D, nên chạy
   lại một ngày vẫn idempotent. DAG hằng ngày không đổi lệnh.
4. **Lần nạp đầu** (stack mới / bootstrap): `--backfill_from 1900-01-01` (CLI) hay conf
   `{"backfill_from": "1900-01-01"}` (Airflow) mở cận dưới; mọi partition ngày có trong nguồn
   được ghi một lần. Tham số chỉ gắn vào job incremental; job full_snapshot từ chối nó.
   Giá trị conf đi qua `macros.ds_format` — không phải ngày thì task fail.
5. **Gold** đọc khoảng partition thay vì một snapshot:
   - model có cửa sổ (customer_360, *_summary, rfm, churn):
     `cob_dt BETWEEN DATE_ADD(cob, -N) AND cob`, N = cửa sổ nghiệp vụ của chính scope đó;
   - model đọc toàn lịch sử (aml_monitoring, fraud_risk_txn, branch_monthly_summary):
     `cob_dt <= cob`.
   Các bộ lọc `txn_date` giữ nguyên. Kết quả **giống hệt** bản snapshot (xem Verification).
6. **Manifest và contract**: số đo của ba bảng dùng `cob_dt <= :cob_dt` (toàn bộ giao dịch tính
   tới ngày đó — cùng nghĩa với một snapshot trước đây); `partition_exists` vẫn đòi đúng ngày.
   `min_row_count` của contract giờ là ngưỡng của MỘT ngày (1.000 / 500 / 500).

## Consequences

**Được**
- Bronze/Silver hằng ngày xử lý một ngày giao dịch, không phải toàn bộ lịch sử. Gold có cửa sổ
  đọc N partition thay vì cả bảng.
- Dung lượng tăng theo số giao dịch mới, không theo số ngày × lịch sử.

**Mất / giới hạn**
- **Giao dịch về muộn**: giao dịch có `txn_date` thuộc ngày D nhưng tới nguồn sau khi D đã nạp sẽ
  bị bỏ sót cho tới khi chạy lại D. Nguồn giả lập không có trường hợp này; hệ thật cần chạy lại
  cửa sổ trượt (vd D-1..D) hoặc trích theo `created_ts`.
- **SK lịch sử**: lần nạp đầu join dimension as-of ngày bootstrap cho mọi partition lịch sử, vì
  dimension chưa có phiên bản nào sớm hơn — y như snapshot cũ làm.
- **Model đọc toàn lịch sử** (aml, fraud, branch_monthly) chưa nhanh hơn: vẫn quét mọi partition
  ≤ cob. Chuyển chúng sang tính theo ngày với cửa sổ nhìn lại là đổi ngữ nghĩa output — quyết định
  riêng.
- **Ngày không có giao dịch** không có partition; guard `require_snapshots` của Gold sẽ dừng —
  đúng ý với ngân hàng thật (ngày làm việc luôn có giao dịch), cần chú ý với dữ liệu giả lập.
- **Plugin**: `cob_dt.BACKFILL_FROM_ARG` nằm trong `airflow/plugins/` → restart scheduler sau khi
  deploy (RUNBOOK).

## Verification

- Unit: `tests/bronze/test_incremental_ingestion.py` (cửa sổ, validate config, cùng cột thời gian
  cho `cob_dt` và cửa sổ, Bronze↔Silver khớp); `tests/gold/test_gold_sql_invariants.py` (fact
  incremental phải đọc khoảng có cận trên cob, cận dưới phủ cửa sổ nghiệp vụ; pin một ngày → fail).
- Stack: xem mục cuối (tương đương Gold trước/sau trên dữ liệu thật).
