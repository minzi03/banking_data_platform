# Bảo Mật — Banking Data Platform

## Báo lỗ hổng

Mở một [GitHub Security Advisory riêng tư](https://github.com/minzi03/banking_data_platform/security/advisories/new) thay vì issue công khai, nếu vấn đề có thể ảnh hưởng người khác.

Với dự án này, phần lớn báo cáo sẽ thuộc một trong hai nhóm: **secrets lọt vào repo** hoặc **PII lộ qua tầng serving**. Hai mục dưới mô tả hiện trạng để bạn biết cái gì đã được xử lý và cái gì là rủi ro đã chấp nhận.

---

## Phạm vi

Đây là **dự án portfolio chạy local bằng Docker Compose**, không phải hệ thống production. Toàn bộ dữ liệu là **dữ liệu sinh tổng hợp** — không có khách hàng thật, giao dịch thật, hay thông tin cá nhân thật nào trong repo.

Điều đó thu hẹp phạm vi rủi ro, nhưng không làm nó bằng không: repo là **công khai**, và các mẫu code ở đây có thể bị sao chép vào nơi có dữ liệu thật.

---

## Secrets

### Hiện trạng

`.gitignore` chặn: `docker/secrets/`, `*.pem`, `*.der`.

7 file secret đã được gỡ khỏi HEAD ngày 2026-09-14 (`git rm --cached`, giữ trên đĩa).

### Rủi ro đã chấp nhận — nêu rõ, không giấu

**Secrets vẫn còn trong lịch sử Git.** Ai clone repo cũng nhận được mọi phiên bản đã commit. Gỡ file khỏi HEAD **không** khắc phục một secret đã publish.

Quyết định không rewrite lịch sử là có chủ ý, với lý do đầy đủ trong [`docs/technical-debt.md`](docs/technical-debt.md) TD-3:

```text
- Toàn bộ credential chỉ thuộc docker stack local
- Không truy cập được dịch vụ bên ngoài nào
- OpenMetadata JWT key là auto-generated per-stack, không tái sử dụng
- Rewrite lịch sử gây rủi ro cho tính toàn vẹn repo và các tham chiếu PR/branch
```

Tương tự, mật khẩu Postgres local của Debezium (`CDCPassword123`) vẫn xuất hiện inline ở ba chỗ trong HEAD: `code_etl/cdc/register_connectors.py`, `airflow/dags/cdc/cdc_register_connectors_dag.py` (dạng default cho biến môi trường), và `DEMO_GUIDE.md`. Đây là **cùng một giá trị với `docker/.env`** và chỉ mở được Postgres trong stack local.

### Quy tắc khi đóng góp

- Không commit credential, kể cả credential local. Dùng biến môi trường có default, đừng hardcode.
- Nếu lỡ commit secret: **báo trước, đừng tự ý force-push**. Xử lý một secret đã publish khác với xoá một file.
- Không `git add -f` để vượt `.gitignore`.

---

## PII và dữ liệu nhạy cảm

Dữ liệu là tổng hợp, nhưng mô hình dữ liệu được thiết kế như thể nó là thật — đó là điểm mấu chốt của dự án.

Cơ chế đang có:

| Cơ chế | Vị trí |
|---|---|
| Column masking | `governance/` · `ops_pii_masking_daily_dag` |
| RBAC | module `governance/rbac.py` |
| Audit trail | module `governance/audit.py` |
| Masking ở tầng serving | `full_name_masked` trong `mart_customer_360` |

**Chưa có tài liệu**: `PII_INVENTORY.md` (cột nào là PII, masking ở tầng nào, ai xem được bản gốc) và `RBAC_MATRIX.md`. Xem [`docs/DOCUMENTATION_PLAN.md`](docs/DOCUMENTATION_PLAN.md) §3 nhóm F.

Nếu bạn mang mẫu code từ đây sang hệ thống có dữ liệu thật: masking hiện áp ở tầng Gold/serving, **không** ở Bronze. Bronze giữ giá trị gốc. Đó là lựa chọn hợp lý cho một lakehouse có kiểm soát truy cập theo tầng, nhưng sẽ sai nếu Bronze của bạn ai cũng đọc được.

---

## Kiểm tra tự động

CI chạy job `Security Scan` trên mọi PR. Ngoài ra `tests/governance/` chứa các test bắt buộc về contract, RBAC và audit.

Lưu ý một giới hạn đã biết: quét tự động bắt được secret **mới** thêm vào, không bắt được secret **đã** nằm trong lịch sử.

---

## Những gì dự án này KHÔNG bảo đảm

Nêu rõ để không ai hiểu nhầm:

- Không có kiểm thử xâm nhập
- Không có mô hình hoá mối đe doạ (threat model)
- Không có quản lý secret cấp production (Vault, KMS, secret rotation)
- Chưa có tài liệu ánh xạ tuân thủ BCBS 239 / SBV tới cài đặt cụ thể — DAG báo cáo đã có, bảng ánh xạ thì chưa

Các khoảng trống này nằm trong [`docs/ROADMAP.md`](docs/ROADMAP.md) và [`docs/DOCUMENTATION_PLAN.md`](docs/DOCUMENTATION_PLAN.md), không bị bỏ quên.
