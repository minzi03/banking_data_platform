# ADR-0014 — Kimball star schema, Data Vault 2.0 chỉ ở mức tài liệu ánh xạ

**Status**: Accepted
**Ngày**: 2026-09-07 · chuyển thể thành ADR 2026-09-22
**Liên quan**: [`../DATA_VAULT_MAPPING.md`](../DATA_VAULT_MAPPING.md)

---

## Context

Silver dùng **Kimball star schema**: 10 dimension (8 SCD1 + 2 SCD2) và 6 fact table. Gold xây 14 mart trên đó.

**Data Vault 2.0** là phương pháp thay thế, và thị trường tuyển dụng ngân hàng có hỏi: `Data Vault` xuất hiện **44 lần** trong corpus JD — nhiều hơn `Great Expectations` (19 lần). Hai ngân hàng nêu đích danh trong JD: OCB (*"Raw Vault + Business Vault + PIT/Bridge"*) và KienlongBank.

Data Vault thắng Kimball ở đúng những chỗ ngân hàng quan tâm:

| Khía cạnh | Kimball | Data Vault 2.0 |
|---|---|---|
| Lịch sử hoá | SCD Type 1/2 | Satellite kèm load date |
| Nạp dữ liệu | Truncate & reload / SCD | Insert-only (append-only) |
| Đổi schema | Cần DDL | Additive — thêm Satellite mới |
| Audit trail | Hạn chế | Đầy đủ (`load_date`, `record_source`) |
| Hợp nhiều nguồn | Khó | Hợp tự nhiên |
| Hiệu năng đọc | Tối ưu sẵn | Cần PIT/Bridge mới truy vấn được |

## Decision

**Giữ Kimball. Không implement Data Vault.** Thay vào đó viết `DATA_VAULT_MAPPING.md` (188 dòng) ánh xạ star schema hiện tại sang Hub/Link/Satellite, kèm DDL ví dụ và quy tắc sinh hash key.

Lý do: dự án này là **analytics/portfolio một người**, đúng hai ô mà chính tài liệu ánh xạ khuyến nghị Kimball:

```text
Analytics/BI project   → Kimball (đơn giản hơn, nhanh hơn)
Small team / MVP       → Kimball (ít overhead)
```

Implement Data Vault sẽ thêm một tầng Raw Vault + Business Vault + PIT/Bridge cho **cùng một tập dữ liệu**, làm chậm mọi query mà không giải quyết vấn đề nào đang có.

Nhưng **bỏ qua hoàn toàn cũng sai**, vì nhu cầu thị trường có thật. Tài liệu ánh xạ trả lời được câu hỏi phỏng vấn — *"hệ thống của bạn sẽ trông thế nào theo DV2.0?"* — mà không phải trả giá vận hành.

## Consequences

**Được**

- Query Gold đơn giản và nhanh: không cần PIT hay Bridge table.
- SCD2 đã phủ nhu cầu lịch sử hoá cho hai dimension cần nó.
- Có câu trả lời cụ thể, có DDL, cho JD yêu cầu Data Vault — thay vì "tôi chưa dùng bao giờ".
- Chi phí bằng 188 dòng tài liệu, không phải một tầng kiến trúc.

**Mất**

- **Audit trail yếu hơn.** Kimball không ghi `record_source` cho từng thuộc tính. Với ngân hàng thật, đây là khoảng trống thực sự chứ không phải chi tiết học thuật.
- Thêm nguồn thứ hai cho cùng một thực thể sẽ đau. Data Vault sinh ra để giải quyết đúng việc đó.
- Đổi schema cần DDL, không additive được.
- **Tài liệu ánh xạ chưa từng được kiểm chứng.** Nó là thiết kế trên giấy: DDL ví dụ chưa bao giờ chạy, hash key chưa bao giờ sinh. Nó đủ để thảo luận, **không** đủ để khẳng định "đã triển khai Data Vault".

## Evidence

```text
docs/DATA_VAULT_MAPPING.md              188 dòng: Hub/Link/Satellite/PIT/Bridge + DDL ví dụ
docs/JD_MARKET_ANALYSIS.md §5.5         Data Vault 44 lần, Kimball/Inmon 31 lần trong corpus
code_etl/silver/dims/                   10 dimension Kimball, 2 trong đó SCD2
```

Ranh giới cần giữ: tài liệu này chứng minh **hiểu** Data Vault, không chứng minh **đã dùng**. Trình bày nó như đã triển khai sẽ là đúng loại tuyên bố không kiểm chứng mà dự án tồn tại để chống.
