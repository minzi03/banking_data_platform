# Business Domain — Banking Data Platform

> Cập nhật: 2026-09-24 · Số liệu lấy từ [`metrics-manifest.yaml`](../evidence/metrics-manifest.yaml)
> (verified, `cob_dt` 2026-09-22) và từ code. Chỗ nào chỉ là thiết kế chưa chạy, tài liệu ghi rõ.

Tài liệu này trả lời bốn câu hỏi, cho người **chưa đọc code**:

1. Dự án mô phỏng **ngân hàng nào**, với **dữ liệu gì**?
2. Dữ liệu đi qua những **khái niệm nghiệp vụ** nào (ngày nghiệp vụ, lịch sử khách hàng, giao dịch)?
3. Tầng Gold **trả lời câu hỏi nghiệp vụ nào**, bằng **quy tắc gì** — và quy tắc đó tốt tới đâu?
4. Dự án đã đi **từ đâu tới đây**, và còn **thiếu gì** so với ngân hàng thật?

Tra cứu cột và kiểu dữ liệu: [`DATA_DICTIONARY.md`](DATA_DICTIONARY.md). Thuật ngữ:
[`GLOSSARY.md`](GLOSSARY.md). Chi tiết từng bảng nguồn: [`data-input-documentation.md`](data-input-documentation.md).

---

## 1. Bối cảnh — một ngân hàng bán lẻ Việt Nam, thu nhỏ

Dự án mô phỏng **một ngân hàng thương mại bán lẻ ở Việt Nam**: khách hàng cá nhân mở tài khoản,
gửi tiết kiệm, vay, dùng thẻ và giao dịch qua quầy, ATM, internet banking và mobile banking.

**Dữ liệu là dữ liệu sinh ra, không phải dữ liệu thật.** `data_generator/` sinh toàn bộ, theo
[`seed_config.yaml`](../../data_generator/config/seed_config.yaml):

| Thực thể | Số lượng | Ghi chú |
|---|---:|---|
| Chi nhánh | 100 | 3 miền: Bắc 35%, Trung 20%, Nam 45% |
| Sản phẩm | 30 | tiền gửi, vay, thẻ |
| Khách hàng | 10.000 | RETAIL / PRIORITY / VIP |
| Tài khoản | 30.000 | CASA và tiền gửi có kỳ hạn |
| Sổ tiết kiệm | 15.000 | kỳ hạn 1–36 tháng |
| Khoản vay | 5.000 | kèm lịch trả nợ từng kỳ |
| Thẻ | 6.000 | DEBIT / CREDIT / PREPAID |
| Giao dịch tài khoản | 1.200.000 | |
| Giao dịch thẻ | 600.000 | |
| Giao dịch online | 500.000 | có nhãn gian lận, tỷ lệ ~0,8% |
| Nhân viên | 1.800 | |

- **Khoảng thời gian:** 2020-01-01 → 2025-12-31 (6 năm).
- **Việt Nam hoá:** họ tên tiếng Việt, số CCCD 12 chữ số, số điện thoại `0xxxxxxxxx`, tiền VND,
  10 thành phố lớn, mã MCC theo ISO 18245, thương hiệu thẻ gồm cả NAPAS.
- **Số tiền** rút theo phân phối **log-normal** (từ PR #21), không phải phân phối đều: đa số giao
  dịch nhỏ, một đuôi dài giao dịch lớn — giống thực tế hơn, và làm các ngưỡng AML có ý nghĩa.

**Quy mô:** khoảng 0,01–0,1% một ngân hàng TMCP lớn ở Việt Nam. Đủ để các bài toán kỹ thuật
(partition, incremental, CDC, grain) là thật; không đủ để nói về hiệu năng ở quy mô production.

---

## 2. Ba phân hệ nguồn

Nguồn là PostgreSQL, chia theo phân hệ như một ngân hàng thật chia hệ thống.

### 2.1 Core Banking — tài khoản, tiền gửi, vay

| Bảng | Nghiệp vụ | Quy tắc đáng chú ý |
|---|---|---|
| `branch` | Mạng lưới chi nhánh | 8% đã đóng cửa |
| `product` | Danh mục sản phẩm | nhóm DEPOSIT / LOAN / CARD |
| `customer` | Hồ sơ khách hàng (CIF) | có `kyc_status`, `customer_segment`; mang **PII** (CCCD, họ tên, SĐT, email, địa chỉ, ngày sinh) |
| `account` | Tài khoản | CASA 55%, có kỳ hạn 45%; trạng thái ACTIVE / CLOSED / FROZEN |
| `deposit` | Sổ tiết kiệm | lãi 3,0–8,5%/năm; 10% rút trước hạn |
| `loan` | Khoản vay | ACTIVE / CLOSED / OVERDUE / WRITTEN_OFF — 5% là nợ đã xoá |
| `loan_payment` | Từng kỳ trả nợ | 5% trả trễ (có phạt), 2% bỏ kỳ |
| `txn_account` | Giao dịch tài khoản | nợ/có (`debit_credit` = `'D'` / `'C'`), 5 kênh, số dư sau giao dịch |
| `employee` | Nhân viên | theo chi nhánh |

### 2.2 Card & CRM — thẻ và chăm sóc khách hàng

| Bảng | Nghiệp vụ |
|---|---|
| `card` | Thẻ phát hành; chỉ thẻ tín dụng có hạn mức |
| `card_txn` | Giao dịch thẻ, gắn mã MCC |
| `crm_interaction` | Mỗi lần khách liên hệ ngân hàng |

### 2.3 Digital Banking — kênh số

| Bảng | Nghiệp vụ |
|---|---|
| `device` | Thiết bị khách dùng |
| `location` | Địa điểm giao dịch; 5% là **vùng rủi ro cao** |
| `online_transaction` | Giao dịch online, **có nhãn `is_fraud` và `fraud_reason`** |
| `support_ticket` | Yêu cầu hỗ trợ |
| `mcc_code` | Danh mục ngành hàng (ISO 18245) |

### 2.4 Cái gì được đưa lên lakehouse, cái gì không

**17 bảng** được nạp vào Bronze (manifest: `bronze.batch_tables = 17`) — tất cả bảng ở trên.

Generator còn sinh những bảng **không** được nạp: `standing_order` (lệnh chi định kỳ),
`merchant`, và ba bảng AML (`aml_rule`, `aml_alert`, `aml_customer_risk`). Chúng tồn tại ở
PostgreSQL nhưng không có pipeline nào đọc — đừng tìm chúng ở Silver hay Gold.

---

## 3. Khái niệm nghiệp vụ mà mọi tầng dùng chung

### 3.1 `cob_dt` — ngày chốt sổ

`cob_dt` (*close of business date*) là **ngày nghiệp vụ** mà một lượt chạy xử lý — giống ngày
chốt sổ cuối ngày của ngân hàng. Mọi bảng Bronze/Silver/Gold đều partition theo `cob_dt`.

- **Bronze là snapshot đầy đủ theo ngày:** mỗi `cob_dt` chứa toàn bộ bảng nguồn tại thời điểm đó.
  Hệ quả nghiệp vụ quan trọng: cùng một giao dịch xuất hiện ở **mọi** partition. Đếm `COUNT(*)`
  qua nhiều ngày sẽ đếm trùng — đó chính là lỗi từng làm dự án công bố **4,6M** giao dịch; con số
  đúng là **2,3M** giao dịch không trùng trong một snapshot (xem §8).
- **Mọi truy vấn Gold ghim đúng một `cob_dt`.** Một lượt chạy chỉ được đọc snapshot của ngày nó xử lý.
- **Ba tầng phải cùng ngày.** Manifest kiểm cả 17 bảng Bronze đều có snapshot đúng ngày
  (`bronze_every_table_at_cob_dt`) — vì bảng dimension nạp lại cho ngày cũ từng làm 13/17 bảng
  lệch ngày mà không kiểm tra nào báo (TD-14).

### 3.2 Giờ lưu trữ và ngày nghiệp vụ

- **Thời điểm** (timestamp) lưu ở **UTC**.
- **Ngày nghiệp vụ** được **suy ra tường minh** theo giờ Việt Nam (UTC+7): giao dịch lúc 01:30
  sáng ngày 2 giờ Việt Nam được lưu là 18:30 **ngày 1** UTC — nhưng ngày nghiệp vụ của nó là
  **ngày 2**, vì khách giao dịch vào ngày 2.
- Spark bắt buộc chạy session UTC; session khác làm job dừng ngay. Trước khi có quy tắc này,
  Spark và Trino tính ra **hai ngày khác nhau** cho cùng một dòng (ADR-0004).

### 3.3 Lịch sử khách hàng và tài khoản — SCD

Ngân hàng cần trả lời "*tại thời điểm X, khách này thuộc chi nhánh nào, phân khúc nào?*" — nên
một số thay đổi phải **giữ lịch sử** chứ không ghi đè.

| Dimension | Kiểu | Thay đổi được giữ lịch sử |
|---|---|---|
| `dim_customer` | **SCD2** | SĐT, email, địa chỉ, thành phố, quận, phân khúc, trạng thái KYC, chi nhánh |
| `dim_account` | **SCD2** | số dư, trạng thái, ngày đóng |
| 8 dimension còn lại (chi nhánh, sản phẩm, thẻ, nhân viên, thiết bị, địa điểm, tiền gửi, khoản vay) | **SCD1** | ghi đè — chỉ cần giá trị hiện tại |

Mỗi phiên bản SCD2 có `effective_from` / `effective_to` / `is_current`. Manifest kiểm: không khách
nào có hai phiên bản hiện hành, và không có hai khoảng thời gian chồng nhau.

### 3.4 Hai cách dữ liệu đến: theo ngày và gần thời gian thực

| Đường | Tần suất | Phủ những gì | Dùng cho |
|---|---|---|---|
| **Batch** | Hằng ngày, Bronze lúc 02:00 | 17 bảng | Mọi báo cáo và mart Gold |
| **CDC** | Liên tục, gộp mỗi 10 phút | 6 bảng: khách hàng, tài khoản, giao dịch tài khoản, tài khoản thẻ, giao dịch thẻ, giao dịch online | Trạng thái **hiện tại** của khách hàng và tài khoản |

CDC cho câu hỏi "*số điện thoại / số dư của khách này **bây giờ** là gì?*" mà không phải chờ
lượt batch đêm. Độ trễ đo được: **median 409,8 giây** từ lúc commit ở PostgreSQL tới lúc đọc được
ở Silver, khoảng 65,9–576,2 giây. Phần lớn là thời gian chờ lượt gộp kế tiếp (chu kỳ 10 phút) —
**đây không phải real-time**. Chi tiết: [`cdc-pipeline.md`](../02-architecture/cdc-pipeline.md), ADR-0010.

---

## 4. Tầng Silver — dữ liệu đã làm sạch, mô hình hoá

Silver có **10 dimension** và **6 fact**, cộng 2 bảng *current-state* từ CDC.

| Fact | Grain (mỗi dòng là) | Câu hỏi phục vụ |
|---|---|---|
| `fact_txn_account` | một giao dịch tài khoản | dòng tiền, hành vi giao dịch |
| `fact_card_txn` | một giao dịch thẻ | chi tiêu thẻ theo ngành hàng |
| `fact_online_transaction` | một giao dịch online | kênh số, **nhãn gian lận** |
| `fact_loan_payment` | một kỳ trả nợ | trả trễ, bỏ kỳ |
| `fact_crm_interaction` | một lần tương tác | mức độ gắn kết |
| `fact_support_ticket` | một yêu cầu hỗ trợ | trải nghiệm khách hàng |

**2,3M giao dịch tài chính** = 1,2M tài khoản + 600K thẻ + 500K online, không trùng, trong một
snapshot (manifest: `curated_financial_transactions`).

---

## 5. Tầng Gold — câu hỏi nghiệp vụ và quy tắc trả lời

**14 mart**, nhóm theo người dùng nghiệp vụ. Mart khách hàng có **một dòng cho mỗi khách mỗi
ngày** — manifest kiểm grain này (`gold_grain_no_duplicates`). Mart rủi ro chấm **từng giao dịch**;
mart chi nhánh tính theo chi nhánh.

### 5.1 Khách hàng 360 — "khách này là ai với ngân hàng?"

`mart_customer_360` gộp **9 nguồn Silver** thành một hồ sơ ~40 cột:

| Nhóm | Chỉ số |
|---|---|
| Hồ sơ | tuổi, giới tính, chi nhánh chính, phân khúc, KYC, ngày đăng ký — họ tên **đã che** (`full_name_masked`) |
| Sản phẩm | số tài khoản / thẻ / khoản vay; có thẻ tín dụng? có tiết kiệm? có vay? |
| Tài sản | tổng tiền gửi, dư nợ vay, **AUM** và nhóm AUM |
| Hoạt động | số lượng / giá trị giao dịch 30 ngày, ngày giao dịch cuối, kênh chính |
| Kênh số | giao dịch online 30 ngày, số thiết bị, có hoạt động số không |
| Tương tác | số lần liên hệ CRM 90 ngày |
| Điểm số | điểm R/F/M, phân khúc RFM, cờ churn, cờ cross-sell thẻ tín dụng |

**Nhóm AUM** (*Assets Under Management* — tổng tài sản khách gửi tại ngân hàng):

| AUM | Nhóm |
|---|---|
| ≥ 5 tỷ | VIP |
| ≥ 1 tỷ | PRIORITY |
| ≥ 100 triệu | AFFLUENT |
| còn lại | MASS |

Năm mart tóm tắt đi kèm — số dư, thẻ, vay, sản phẩm, giao dịch — mỗi mart một góc của cùng khách
hàng. `churn_prediction` được đối chiếu với `customer_transaction_summary` mỗi lần sinh manifest:
hai mart tính cùng một con số theo hai đường, phải khớp.

### 5.2 Phân khúc và marketing — "nên nói gì với khách này?"

**RFM** (`rfm_segment`) — chấm khách theo 90 ngày gần nhất:

- **R**ecency: bao lâu từ giao dịch cuối · **F**requency: bao nhiêu giao dịch · **M**onetary: bao nhiêu tiền.
- Mỗi trục chia 5 nhóm bằng `NTILE(5)`, cộng lại thành điểm 3–15:

| Tổng điểm | Phân khúc |
|---|---|
| ≥ 13 | Champions |
| ≥ 10 | Loyal Customers |
| ≥ 7 | Potential Loyalists |
| ≥ 6 | New Customers |
| ≥ 4 | At Risk |
| ≥ 2 | Hibernating |
| còn lại | Lost |

**Nguy cơ rời bỏ** (`churn_prediction`) — theo số ngày từ giao dịch cuối:

| Không giao dịch | Mức | Ứng viên churn? |
|---|---|---|
| > 90 ngày, hoặc chưa từng | High | có |
| > 60 ngày | Medium | không |
| > 30 ngày | Low | không |
| ≤ 30 ngày | Active | không |

Đây là **quy tắc**, không phải mô hình học máy — cột tên `prediction` nhưng không dự đoán gì.

**Cơ hội bán chéo** (`cross_sell_segment`) — khách còn thiếu sản phẩm nào: ưu tiên thẻ tín
dụng, rồi thẻ ghi nợ.

**Chiến dịch** (`campaign_target`) — mỗi khách nhận **một** chiến dịch, xét theo thứ tự:

| Thứ tự | Điều kiện | Chiến dịch |
|---:|---|---|
| 1 | ứng viên churn **và** RFM At Risk / Hibernating | **Retention** — giữ chân |
| 2 | chưa có thẻ tín dụng **và** AUM từ AFFLUENT trở lên | **Cross_Sell_CC** |
| 3 | còn cơ hội bán chéo khác | **Cross_Sell** |
| 4 | RFM Champions / Loyal | **Upsell** |
| 5 | còn lại | **Awareness** |

### 5.3 Rủi ro — "khách hay giao dịch nào đáng lo?"

**Chống rửa tiền** (`aml_monitoring`) — mỗi dòng là một giao dịch tài khoản; năm dấu hiệu
(*typology*) được tính trên **toàn bộ giao dịch của khách trong ngày** rồi gắn vào từng dòng:

| Cờ | Bật khi |
|---|---|
| `high_value_flag` | giao dịch ≥ 200 triệu |
| `structuring_flag` | ≥ 2 giao dịch 50–99,99 triệu trong ngày — chia nhỏ để né ngưỡng báo cáo |
| `velocity_flag` | ≥ 4 giao dịch trong ngày |
| `multi_channel_flag` | ≥ 4 kênh trong ngày |
| `geo_velocity_flag` | ≥ 3 vùng khác nhau trong ngày |

Có từ 2 cờ trở lên thì **sinh cảnh báo** (`alert_generated`).

**Gian lận** (`fraud_risk_txn`) — mỗi dòng là một giao dịch tài khoản, bốn cờ: giao dịch đêm (trước 6h
hoặc sau 22h), số tiền > 100 triệu, số dư âm, và **bất thường** (vượt trung bình + 3 độ lệch chuẩn
của chính khách đó).

**Danh mục cho vay** (`loan_portfolio_risk`) — theo chi nhánh: số khoản vay đang hoạt động / quá
hạn, **tỷ lệ quá hạn**, tỷ lệ trả trễ.

**Cả hai mart AML và gian lận có cột `is_fraud` làm đáp án** (từ nhãn giao dịch online) — để **đo**
các quy tắc, không phải để huấn luyện. Kết quả đo (ROADMAP 2.3), trên tỷ lệ gian lận nền 0,096%:

| Quy tắc | Độ nâng (lift) | Đọc là |
|---|---:|---|
| `geo_velocity_flag` | **12,45×** | quy tắc duy nhất có tín hiệu thật |
| `velocity_flag` | 1,13× | gần như ngẫu nhiên |
| `high_value_flag` | 0,96× | ngẫu nhiên |
| `structuring_flag`, `multi_channel_flag` | 0 | không bắt được ca nào |
| Cảnh báo AML cuối cùng | 0 / 1.358 | không cảnh báo nào trúng |
| Mọi cờ của `fraud_risk_txn` | 0,90–1,13× | ngẫu nhiên; `neg_balance_flag` không bao giờ bật |

**Vì sao yếu:** generator chỉ mô phỏng gian lận ở **kênh online** (địa điểm rủi ro + số tiền),
còn các quy tắc chấm **giao dịch tài khoản** — vốn không có quan hệ nào với gian lận trong dữ liệu
sinh. Đây là phát hiện có giá trị, không phải lỗi che giấu: nó cho thấy quy tắc phải được đo
trên đúng kênh có tín hiệu.

### 5.4 Chi nhánh theo thời gian

`branch_monthly_summary` — mỗi chi nhánh mỗi tháng: số giao dịch, tổng tiền vào / ra. Manifest
tính lại mart này **bằng Trino** và so với kết quả **của Spark**: hai engine độc lập phải ra cùng
số, lệch là lỗi (`branch_monthly_cross_engine_reconciles`).

### 5.5 Lỗi nghiệp vụ đã biết trong Gold

| Mart | Vấn đề | Hệ quả |
|---|---|---|
| `aml_monitoring` | so `debit_credit` với `'CREDIT'` / `'DEBIT'`, trong khi nguồn chỉ có `'D'` / `'C'` (CHECK ở `01_ddl_core_banking.sql`). `branch_monthly_summary` dùng đúng `'C'` / `'D'` | `total_credit` và `total_debit` luôn bằng **0**. Năm cờ AML không dùng hai cột này nên không bị ảnh hưởng |
| `loan_portfolio_risk` | cột `npl_proxy` là **tổng dư nợ**, không phải nợ xấu | Tên gây hiểu nhầm — đừng đọc nó là tỷ lệ NPL |

Phát hiện khi viết tài liệu này (2026-09-24), chưa sửa.

---

## 6. Ai dùng dữ liệu, và thấy được gì

### 6.1 Tầng serving

**13 bảng** `serving.*_current` do dbt dựng trên Trino — bản **ngày hiện hành** của các mart Gold,
cho các ứng dụng đọc mà không phải biết `cob_dt`:

| Người dùng | Công cụ | User Trino | Đọc được |
|---|---|---|---|
| Phân tích, báo cáo | Superset | `superset` | chỉ `serving` |
| Ứng dụng | Customer API (FastAPI) | `customer_api` | chỉ `serving` |
| Dashboard nội bộ | Streamlit | `streamlit` | chỉ `serving` |
| Mô hình | ML / MLflow | `ml` | chỉ `serving` |
| Dựng serving | dbt | `dbt` | đọc Gold, ghi `serving` |

Serving lấy `cob_dt` từ biến dbt chứ không lấy `MAX(cob_dt)`: thiếu snapshot của ngày thì build
**thất bại**, thay vì im lặng phục vụ số của hôm qua.

### 6.2 Dữ liệu cá nhân

**31 bảng** chứa dữ liệu cá nhân ([`PII_INVENTORY.md`](../06-security-compliance/PII_INVENTORY.md)).
Sáu cột nhạy cảm nhất — CCCD, họ tên, SĐT, email, địa chỉ, ngày sinh — được xử lý:

- **Gold / serving:** che **lúc ghi** — chỉ có `full_name_masked`.
- **Bronze / Silver:** lưu bản gốc, Trino che **lúc đọc** với mọi role không phải admin / ETL
  (CCCD chỉ còn 4 số cuối, ngày sinh chỉ còn năm…).

Ai thấy bản gốc ở bảng nào: [`RBAC_MATRIX.md`](../06-security-compliance/RBAC_MATRIX.md), sinh tự
động từ `governance/rbac.py`.

**Giới hạn:** Trino **chưa xác thực người dùng** — tin tên user client tự khai. Spark và MinIO
**không đi qua Trino**. Đủ để chặn lộ vô ý; không đủ để chặn người cố ý (ADR-0015).

---

## 7. Dữ liệu đáng tin tới đâu

| Cơ chế | Kiểm gì | Loại |
|---|---|---|
| **34 data contract** | bảng thật khớp schema đã cam kết | phát hiện |
| **Data quality** — 88 check, 29 bảng, 9 loại | null, trùng, khoảng giá trị, tham chiếu… | phát hiện, không chặn ([`DATA_QUALITY.md`](../05-quality/DATA_QUALITY.md)) |
| **Quarantine** | tách dòng vi phạm quy tắc nghiệp vụ | cách ly |
| **Evidence manifest** — 23 invariant | grain, SCD2, đối chiếu chéo mart, đồng bộ ngày | chặn công bố số liệu sai |
| **34 test tích hợp Trino** | pipeline chạy thật trên dữ liệu thật | chặn merge PR |
| **Lineage** — 75 quan hệ | bảng nào sinh từ bảng nào, lấy từ cấu hình job | tra cứu ([`LINEAGE.md`](LINEAGE.md)) |

Mọi con số công bố trong README đều là **hình chiếu** của manifest: sửa tay một con số trong
README mà không đo lại thì CI đỏ ([`EVIDENCE_MANIFEST.md`](../05-quality/EVIDENCE_MANIFEST.md)).

---

## 8. Hành trình dự án

| Giai đoạn | Thời gian | Điều chính |
|---|---|---|
| **Dựng nền → portfolio-v1.0** | 2026-08-06 → 08-09 | Generator, pipeline Bronze → Silver → Gold, dbt, Streamlit; kiến trúc Medallion trên Spark + Iceberg + MinIO. Tag `portfolio-v1.0` ngày 08-09, đóng băng tính năng |
| **portfolio-v1.1 — đúng số liệu** | 2026-09-06 → 09-07 (tag 09-06) | Sửa fan-out ở RFM / churn / card summary; giao dịch **4,6M → 2,3M** (đếm trùng qua snapshot); chuẩn hoá giờ UTC và ngày nghiệp vụ; serving chuyển sang dbt trên Trino; ra đời evidence manifest; 34 test Trino thành cổng chặn PR |
| **Tài liệu hoá** | 2026-09-21 | ADR, runbook sự cố, data dictionary và contract sinh tự động; dựng đủ 4 mart Gold còn thiếu; thêm cờ `geo_velocity` cho AML |
| **Thực tế hoá dữ liệu → portfolio-v2.0** | 2026-09-22 (tag cùng ngày) | Số tiền log-normal; hiệu chỉnh ngưỡng AML và sửa lỗi grain; kiểm kê 31 bảng PII; gỡ salt băm PII bị commit |
| **Quản trị thật sự chạy** | 2026-09-23 → 09-24 | Trino thực thi RBAC và che PII sinh từ `rbac.py`; job DQ chạy được trên Spark (trước đó chưa từng chạy); nhãn `is_fraud` để đo quy tắc; contract validation chạy thật; 34 contract khớp bảng thật; kiểm đồng bộ ngày của cả 17 bảng Bronze; lineage có bảng và có job ghi |

Lịch sử chi tiết: [`CHANGELOG.md`](../../CHANGELOG.md), PR #1–#55,
[`technical-debt.md`](../05-quality/technical-debt.md) (TD-1 → TD-14).

**Mẫu lặp lại của cả hành trình:** nhiều thứ đã *có* từ đầu — RBAC, DQ, contract, lineage — nhưng
**không chạy** hoặc **đo sai**. Phần lớn công việc sau v1.0 là biến "có file" thành "có bằng chứng
chạy được", và công bố số đo thật kể cả khi nó không đẹp (410 giây, lift 0,96×).

---

## 9. So với một ngân hàng thật

| Có — ở mức mô phỏng | Chưa có |
|---|---|
| Hồ sơ khách hàng, KYC, phân khúc | Sổ cái (General Ledger), bút toán kép |
| Tài khoản, tiền gửi có kỳ hạn, vay và lịch trả nợ | Tính lãi hằng ngày, lãi kép, bảng lãi phạt |
| Thẻ và giao dịch thẻ theo MCC | Sao kê thẻ, chu kỳ thanh toán, điểm thưởng |
| Giao dịch đa kênh, kênh số | Thanh toán liên ngân hàng (NAPAS, SWIFT, RTGS) |
| Quy tắc AML và gian lận, có đáp án để đo | Mô hình chấm điểm tín dụng, sàng lọc PEP, báo cáo STR |
| Rủi ro danh mục vay theo chi nhánh | Basel III, IFRS 9 ECL |
| Kiểm soát truy cập, che PII | Xác thực người dùng; báo cáo NHNN (Thông tư 13/2023); `REGULATORY_MAPPING.md` |

Dự án cố ý tập trung vào **kỹ thuật dữ liệu** trên một miền nghiệp vụ đủ thật, không cố mô phỏng
toàn bộ ngân hàng. Hướng tiếp theo đã xếp hạng: [`ROADMAP.md`](../09-analysis/ROADMAP.md) — gồm
bốn KPI rủi ro còn thiếu (mức dùng hạn mức tín dụng, thẻ ngủ đông, giao dịch bất thường theo MCC,
heatmap vùng × ngành hàng).
