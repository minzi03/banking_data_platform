# ADR-0015 — Access control của Trino sinh từ `rbac.py`, danh tính do client tự khai

**Status**: Accepted
**Ngày**: 2026-09-23
**Liên quan**: [`0002`](0002-cross-engine-catalog-naming.md) · [`0008`](0008-evidence-manifest-as-verifier.md)

---

## Context

Repo có hai nơi mô tả quyền truy cập lakehouse, và không nơi nào được thực thi:

- `governance/rbac.py`: 5 role, 5 user, 4 luật che cột. Không file nào ngoài test `import` nó.
- `docker/init_trino/access-control.properties`: 195 dòng luật. Trino không nạp file này vì bốn lý do độc lập:
  1. Compose chỉ mount `init_trino/catalog`, còn file này nằm ở thư mục cha.
  2. File không có khoá `access-control.name`, và viết luật thành các cặp `key=value` lặp lại thay vì file luật JSON.
  3. Luật khớp theo group, nhưng không có group provider nào được cấu hình.
  4. 5/9 luật che nhắm vào cột Gold không tồn tại.

Không có file `etc/access-control.properties` thì Trino dùng access control `default`: mọi client đọc được `cccd` nguyên bản ở Silver và Bronze.

Hai bản mô tả còn **lệch nhau và cùng sai**. Cả hai che `gold.mart_customer_360.{full_name, phone, email, cccd}`, trong khi Gold chỉ có `full_name_masked`. Riêng `rbac.py` còn cho `data_steward` kế thừa quyền đọc Silver của `analytics` mà **không** kế thừa luật che.

Mọi client (dbt, Superset, API, Streamlit, ba job ML, freshness exporter, script manifest) kết nối qua HTTP với user `admin`. Trino chưa có xác thực.

## Decision

**1. `governance/rbac.py` là nguồn sự thật duy nhất.** `scripts/generate_trino_access_control.py` sinh `docker/init_trino/rules.json` từ đó. Test chặn drift. Không sửa tay `rules.json`.

**2. Luật khớp theo tên user, không qua group.** Mỗi tổ hợp role sinh một khối luật với regex user riêng, và mỗi user chỉ thuộc một khối. Vì Trino áp **luật khớp đầu tiên**, cách này làm thứ tự giữa các khối không đổi kết quả. Không cần group provider, nên bớt được một file cấu hình có thể quên mount.

**3. Mỗi loại client một role, mỗi client một user.**

| Role | User | Quyền |
|---|---|---|
| `serving_builder` | `dbt` | đọc Gold, tạo và ghi bảng trong `serving` |
| `serving_consumer` | `superset` · `customer_api` · `streamlit` · `ml` | chỉ đọc `serving` |
| `observer` | `freshness_exporter` · `manifest_collector` | đọc mọi tầng, PII bị che |
| `admin` | `admin` · `trino` · `trino_admin` | toàn quyền |

**4. Luật che nằm trên bảng thật sự mang PII**: Silver `dim_customer` và `dim_customer_current` cho `analytics`; thêm Bronze `core_customer` và `core_customer_cdc` cho `observer`. Luật che được kế thừa theo `parent_roles`.

**5. User `trino` có quyền admin.** Đây là user mặc định của Trino CLI khi chạy `docker exec <trino> trino`, cách CI, benchmark và RUNBOOK đang dùng. Ai exec được vào container Trino thì đã kiểm soát server (sửa được `rules.json`), nên cấp admin cho tên này không mở thêm quyền gì.

**6. Chưa xác thực trong ADR này.** Password auth bắt buộc HTTPS, nên phải có cert và đổi khoảng 12 client. Việc đó được tách ra làm thay đổi riêng.

## Consequences

**Được**

- Client làm đúng không còn đọc được PII nguyên bản ở nơi chúng không cần. Tầng tiêu thụ chỉ thấy `serving`.
- `rbac.py` và Trino nói cùng một điều. Test so `RBACManager.has_access` và `get_masked_columns` với các quyết định sinh ra từ `rules.json`, trên mọi user × schema.
- Việc đối chiếu này đã tìm ra một lỗi thật trong `has_access`: quyền ghi không bao hàm quyền đọc, nên theo API Python thì `etl_user` "không đọc được" chính bảng nó ghi.
- Lỗi mask lệch kiểu chỉ lộ ra lúc có người query. Bước CI `Trino access control enforced` query thật mọi bảng có mask trên dữ liệu thật để bắt lỗi này.

**Mất**

- **Chưa phải bảo mật trước người cố ý.** Trino tin tên user trong header `X-Trino-User`, nên ai kết nối được cổng 8080/8085 đều khai được `admin`. Luật chặn truy cập nhầm và thể hiện least privilege, nhưng không thay được xác thực.
- **Spark không đi qua Trino.** Job Spark đọc ghi thẳng Iceberg REST và MinIO, và ai có credential MinIO đọc được file Parquet gốc. Luật này chỉ phủ đường Trino.
- **Catalog `system` luôn mở.** Trino 443 tự nối một luật ẩn "mọi user → `system` ALL" vào cuối danh sách. User lạ vào được catalog nhưng không có luật bảng nào, nên không đọc được bảng nào.
- **Luật che mới phủ nhóm nhạy cảm "Cao nhất" và "Cao" trên bảng khách hàng.** `account_no`, `device_id`, lat/long, tên nhân viên chưa che.
- **`etl_user` tạo được bảng ở mọi schema, kể cả `serving`.** Mô hình cấp `TABLE iceberg.*.* WRITE`, và Trino hiểu quyền tạo bảng là privilege `OWNERSHIP` trên bảng. Test `test_only_dbt_etl_steward_and_admins_create_tables` ghi lại ai đang có quyền này, để mọi lần mở rộng đều hiện ra trong diff.
- Thêm một file sinh ra phải giữ đồng bộ, cùng một script kiểm cần Docker.

## Evidence

- Ngữ nghĩa luật lấy từ tài liệu Trino 443 (*File-based access control*) và mã nguồn `FileBasedSystemAccessControl` / `FileBasedSystemAccessControlModule` 443:
  - `CREATE`, `DROP`, `RENAME TABLE` cần `OWNERSHIP` trên bảng.
  - Owner của schema chỉ quyết định `CREATE` và `DROP SCHEMA`.
  - Catalog `system` có luật ẩn ở cuối danh sách.
- Bộ đánh giá Python trong `tests/governance/test_trino_access_control.py` đã được đối chiếu với chính `FileBasedSystemAccessControl` của Trino 443, nạp `rules.json` thật: **1860/1860 quyết định khớp**, gồm quyền catalog, SELECT, INSERT, tạo/xoá/đổi tên bảng, tạo schema, luật che trên từng cột PII, xem truy vấn của user khác, và đọc system information.
- dbt-trino 1.9.0 (bản đang pin) join `system.metadata.materialized_views` trong mọi lần list relation. Vì vậy mọi user đã khai đều được đọc `system.metadata` và `system.jdbc`. Trino tự lọc các bảng metadata này theo quyền của user đang hỏi.
- Runtime: `scripts/verify_trino_access_control.py`, chạy trong job Trino Integration của CI.
