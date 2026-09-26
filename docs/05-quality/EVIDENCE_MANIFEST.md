# Evidence Manifest — Cơ Chế Chống Số Liệu Bịa

> Cập nhật: 2026-09-22 · Bản đồ tài liệu: [`../INDEX.md`](../INDEX.md)
> Quyết định kiến trúc: [ADR-0008](../02-architecture/adr/0008-evidence-manifest-as-verifier.md)

Đây là cơ chế đặc trưng nhất của dự án, và cũng là thứ dễ hiểu sai nhất. Tài
liệu này giải thích nó hoạt động ra sao, **và những chỗ nó KHÔNG bảo vệ**.

Mọi con số dưới đây đo từ `docs/evidence/metrics-manifest.yaml` tại
`cob_dt 2026-09-22`, không chép lại từ tài liệu khác.

---

## 1. Vấn đề nó giải quyết

README công bố hàng chục con số: 17 source workload, 14 historical Gold table,
2.300.000 giao dịch, 29 Docker service. Mỗi con số là một tuyên bố có thể sai —
và sai theo kiểu **không ai phát hiện**, vì tài liệu không chạy.

Cách thông thường là chép số vào README rồi thỉnh thoảng cập nhật tay. Cách đó
hỏng theo thời gian một cách có hệ thống: code đổi, số ở README đứng yên, không
có gì báo.

Cách thứ hai — sinh số tự động rồi ghi thẳng vào tài liệu — tốt hơn nhưng vẫn
thiếu một thứ: nó không phân biệt được **"đo được"** với **"đo được và đúng"**.

---

## 2. Ba pha: COLLECT → VERIFY → promote

`scripts/generate_metrics_manifest.py` **không phải script dump số. Nó là
verifier.**

```text
COLLECT  →  LUÔN ghi timestamped run artifact vào docs/evidence/generated/
   ↓
VERIFY   →  chạy 23 invariant
   ↓
ERROR == 0 ?
      yes → ghi đè canonical (atomic: .yaml.tmp rồi Path.replace)
      no  → GIỮ canonical cũ, exit 1
```

Điểm mấu chốt nằm ở nhánh `no`: khi có invariant đỏ, manifest canonical **không
bị cập nhật**. Tài liệu giữ nguyên trạng thái đã kiểm chứng lần cuối thay vì
nhận số mới chưa qua kiểm.

Tách COLLECT khỏi VERIFY để lại **forensic evidence**: một lần rebuild fail vẫn
ghi artifact vào `docs/evidence/generated/` thay vì mất trắng. Hiện có 42
artifact ở đó, và thư mục **không được git track** (`.gitignore:79`) — nó là log
chẩn đoán cục bộ, không phải sản phẩm.

### Status phụ thuộc scope

`verified` **chỉ** cấp cho `--scope full`:

```python
if errors:                    status = "failed"
elif scope_name != "full":    status = f"verified_{scope_name}"   # verified_batch
else:                         status = "warning" if warnings else "verified"
```

Một lượt batch sạch là `verified_batch`, không phải bằng chứng toàn platform.

### Provenance không bao giờ bị cờ CLI viết lại

```python
# collect_build_metadata()
# "Cờ được phép quyết định CÓ CHẠY hay không;
#  nó không được phép quyết định SỰ THẬT là gì."
```

Trước đây `--allow-dirty` làm hàm này ghi `git_dirty: False` trên cây bẩn — tức
tác dụng duy nhất của cờ là khiến manifest nói dối về chính nguồn gốc của nó.
Nay `git_dirty` luôn ghi đúng sự thật. Xem §6.2 về trạng thái hiện tại của cờ đó.

---

## 3. Bốn khái niệm phải hiểu đúng

### `declared` vs `value`

```yaml
platform.automated_tests.test_functions:
  value: 655        # ĐO ĐƯỢC
  declared: 655     # KHAI BÁO — con số ta nói là đúng
```

> **Khi lệch, sửa `declared` và README. KHÔNG sửa `value` bằng tay.**
> `value` là kết quả đo; sửa nó là làm giả phép đo.

`declared: null` nghĩa là *"đo nhưng không publish"*. `_static_drift()` bỏ qua
những node này, nên một metric nội bộ không kéo cả manifest sang `warning`.

### `metric_type`

| Loại | Số | Nguồn | Sinh lại được? |
|---|---:|---|---|
| `static` | 24 | đếm file, parse YAML/DDL trong repo | ✅ mỗi lần chạy |
| `runtime` | 11 | query qua Trino trên nền tảng đang chạy | ✅ nếu stack lên |
| `manual` | 1 | đo thủ công, có ngày và phương pháp | ❌ generator **preserve** |

Metric `manual` duy nhất là `cdc_freshness` — đo bằng
`scripts/measure_cdc_freshness.py` (UPDATE PostgreSQL → poll Silver current qua
Trino). Không query được từ catalog, nên generator giữ giá trị cũ thay vì ghi
đè bằng `null`.

**Hệ quả**: metric `manual` **không tự già đi**. Số đo tháng trước trông y hệt
số đo hôm nay nếu không đọc trường ngày. Vì thế
`tests/governance/test_docs_no_stale_claims.py` bắt mọi tài liệu nêu freshness
phải nêu **kèm cadence** (600s) — con số 409,8s không có cadence là vô nghĩa.

### `verification_scope`

```text
batch   skips: cdc.
cdc     skips: bronze.  silver.  gold.  transaction_scale.  serving.
full    skips: (không có)
```

Mỗi scope khai báo **rõ nó bỏ qua gì và vì sao**, ngay trong manifest. Invariant
trỏ vào metric ngoài scope đi vào `skipped` **kèm tên invariant**, không âm thầm
pass.

### `not_collected ≠ verified`

Quy tắc nền, và nó được cưỡng chế trong `evaluate_invariants()`:

```python
if actual is MISSING or actual is None or expected is None:
    bucket.append(f"{inv_id}: {metric_path} chưa có giá trị")
```

Metric chưa đo được là **FAIL**, không phải pass. Thiếu dữ liệu khác với đã
kiểm chứng.

---

## 4. Invariant: 22 chặn, 1 cảnh báo

23 invariant, tất cả toán tử `eq`. Phân bố severity có chủ ý:

```text
error  22   fail → KHÔNG promote canonical
warn    1   fail → vẫn promote, nhưng ghi vào warnings
```

Invariant `warn` duy nhất là `static_metrics_match_declared`
(`metric: __all_static__`, `compare_to: __declared__`), lý do ghi ngay trong
manifest:

> *"Khi lệch, việc cần làm là sửa README + declared, không phải chặn sinh
> manifest."*

Đúng: lệch `value` ↔ `declared` nghĩa là **tài liệu tụt hậu**, không phải nền
tảng hỏng. Chặn việc sinh manifest khi đó sẽ khiến không ai đo được gì cho tới
khi sửa xong README — ngược đời.

Các invariant `error` kiểm những thứ mà sai là dữ liệu sai thật:

| Invariant | Bắt điều gì |
|---|---|
| `snapshot_layers_aligned` | bronze/silver/gold cùng `requested_cob_dt` |
| `scd2_no_overlapping_intervals` | *"invariant mà chỉ nhìn `is_current` không phát hiện được"* |
| `churn_reconciles_amount` | fan-out tái phát giữa hai Gold model |
| `serving_snapshot_alignment` | serving phục vụ đúng snapshot của Gold |
| `worktree_clean` | `manifest.build.git_dirty == false` |

Ba invariant mang ghi chú **đổi chiều có chủ ý** — `legacy_gold_current_retired`
và `branch_monthly_cross_engine_reconciles` nâng từ `warn` lên `error` sau khi
migration xong; `legacy_spark_view_retired` đảo hẳn câu hỏi (trước: view phải
tồn tại; sau: view phải biến mất). Invariant là hợp đồng theo thời điểm, không
phải chân lý vĩnh viễn — nên khi đổi, ghi lý do vào `rationale`.

---

## 5. README là phép chiếu, không phải nguồn

```text
docs/evidence/metrics-manifest.yaml   ← nguồn sự thật (sinh + verify)
              ↓  readme_bindings
          README.md                   ← phép chiếu
```

**18 binding entry → 22 projection được kiểm.** Một metric có thể được chiếu vào
nhiều chỗ, và cả hai chỗ đều phải khớp:

```yaml
- manifest_path: metrics.gold.tables.value
  readme_claims:
    - location: metrics_table
      claim: "| Historical Gold tables     |             14 |"
    - location: executive_summary
      claim: "14 historical Gold models"
```

Bốn metric có hai projection (Gold tables, serving objects, curated
transactions, integration tests) — vì executive summary từng trôi khỏi bảng
metric đúng theo cách sơ đồ mermaid từng trôi khỏi prose.

`verify_readme_metrics.py` khớp **nguyên văn** chuỗi claim, nên đổi tên nhãn mà
quên cập nhật binding cũng bị bắt. Nó chấp nhận nhiều cách trình bày cùng một
giá trị: `2300000` khớp với `"2,300,000"`, `"2.3M"`, `"2.3 million"`.

Hiện **22/22 khớp**.

---

## 6. Những chỗ cơ chế này KHÔNG bảo vệ

Phần quan trọng nhất của tài liệu.

### 6.1 Vòng lặp chỉ khép khi regenerate

`verify_readme_metrics.py` so **README ↔ manifest**, **không** so manifest ↔
thực tế. Manifest cũ vẫn cho 22/22 xanh trong khi cả hai đều đã sai.

> **Đã xảy ra trong chính phiên 2026-09-22**: thêm test mới đưa suite từ 616 lên
> 655 hàm. Verifier vẫn xanh suốt vì README khớp manifest cũ. Chỉ lượt sinh lại
> mới phát hiện.

Nó chỉ chặn được `status: pending` — manifest chưa từng sinh. Manifest **đã sinh
nhưng cũ** thì nó không phân biệt được.

### 6.2 Cây bẩn không promote được — và không có cờ nào nới ra

**Không còn `--allow-dirty`.** Cờ đó từng hứa *"cho phép promote canonical từ
worktree bẩn"* nhưng chưa bao giờ làm được: `worktree_clean` là invariant
severity `error`, và `evaluate_invariants()` không có ngoại lệ nào cho nó. Nên
trên cây bẩn:

```text
errors = ['worktree_clean: manifest.build.git_dirty=True eq False → FAIL']
→ promote_canonical_if_verified() thoát ngay ở `if errors: return False`
→ nhánh kiểm git_dirty BÊN TRONG hàm đó không bao giờ chạy với cây bẩn
```

Cờ không đổi hành vi trong bất kỳ trường hợp nào — cây bẩn vẫn không promote
được, cây sạch thì cờ vô nghĩa. Đã gỡ, vì `CONTRIBUTING.md` và `ROADMAP.md` đều
cấm dùng nó, và `--collect-only` đã phủ đúng nhu cầu hợp lệ *"chạy trên cây bẩn
mà không promote"*.

Cây bẩn giờ bị chặn ở **hai** cổng, thừa một cách có chủ ý:

| Cổng | Ở đâu | Chặn khi |
|---|---|---|
| `worktree_clean` invariant | contract YAML — **dữ liệu sửa được** | luôn, vì `errors` không rỗng |
| nhánh `git_dirty` trong `promote_canonical_if_verified()` | code | chỉ khi caller truyền `errors=[]` |

Cổng thứ hai không bao giờ chạy từ `main()`. Nó được giữ lại vì cổng thứ nhất
sống trong contract: xoá một invariant khỏi YAML không được phép âm thầm mở
đường promote từ cây bẩn.

Dòng cuối khi bị từ chối giờ nói thẳng nguyên nhân sửa được:

```text
KHÔNG promote canonical — 1 blocking invariant fail. Trong đó có worktree BẨN
(git_dirty: true): số đo không quy được về một commit. Commit thay đổi rồi chạy
lại; --collect-only nếu chỉ cần thu evidence. Canonical giữ nguyên; xem run
artifact để triage.
```

> **Quy tắc vẫn giữ**: commit thay đổi trước, rồi mới sinh manifest.

### 6.3 Nền tảng lệch tầng vẫn "đo được"

Generator sẽ chạy và promote ngay cả khi các tầng ở những ngày khác nhau —
**trừ khi** `snapshot_layers_aligned` bắt được.

> **Đã xảy ra**: 2026-09-21, Bronze/Silver ở `09-20`, Gold ở `09-18`, serving ở
> `09-17`. Và không `cob_dt` nào có đủ bốn tầng, vì Bronze dimension là
> full-snapshot nên partition cũ bị ghi đè. Phản xạ sai lúc đó là kết luận
> "nền tảng hỏng"; việc đúng là kiểm alignment từng tầng trước, rồi chạy lại
> Gold + dbt.

> **Xảy ra lần nữa, và lần này invariant không bắt** (2026-09-24). Cả 13 bảng
> dimension Bronze bị nạp lại cho `2026-09-21` — full-snapshot nên mỗi bảng chỉ
> còn đúng snapshot đó — trong khi fact vẫn ở `09-22`. `snapshot_layers_aligned`
> vẫn qua, vì `bronze_max_cob_dt` và `bronze_partition_exists` chỉ đo
> `bronze.core_txn_account`. Lượt `--collect-only` lộ ra qua một metric khác:
> `bronze.snapshot_rows.core_customer.rows` 10000 → 0. Không promote; nạp lại 13
> bảng cho `09-22` rồi mới sinh manifest. Lỗ hổng ghi ở TD-14.
>
> **Đã sửa (TD-14):** query `bronze.tables_at_cob_dt` đếm số bảng Bronze batch có
> đúng `cob_dt`, và invariant `bronze_every_table_at_cob_dt` so nó với số workload
> trong config. Kiểm ngược trên stack: một dimension ở ngày khác → `16 eq 17 → FAIL`,
> `missing = 'core_mcc_code'`, trong khi `layers_aligned` vẫn `True`.

Cách kiểm an toàn trước khi promote:

```bash
py -3 scripts/generate_metrics_manifest.py --cob-dt 2026-09-22 --scope full --collect-only --continue-on-error
```

`--collect-only` thu evidence và chạy invariant **mà không promote**;
`--continue-on-error` báo cáo **mọi** lỗi một lượt thay vì dừng ở lỗi đầu.

### 6.4 Binding chỉ neo README

`docs/02-architecture/architecture.md` từng khai `476` test trong khi manifest
ghi `616`, và không binding nào bắt.

`tests/governance/test_docs_no_stale_claims.py` phủ thêm `ARCHITECTURE.md` và
`docs/02-architecture/architecture.md` — nhưng bản đầu của nó dùng **so khớp
chuỗi con trên cả file**, nên đã cho một false green: chuỗi `"616"` tình cờ xuất
hiện đúng một lần ở chỗ khác. Nay nó trích con số **ngay cạnh** cụm claim rồi so
từng cái.

Ba file được phủ (README, ARCHITECTURE.md, architecture.md). Mọi tài liệu khác —
kể cả tài liệu này — không có gì canh.

### 6.5 Một metric có thể đúng mà vẫn gây hiểu nhầm

Manifest ghi **cả hai** cách đếm test, và nói thẳng rằng việc chọn cái nào là
biên tập:

```yaml
test_functions:         value: 655   # def test_*
collected_pytest_nodes: value: 886   # sau khi parametrize nở ra
readme_primary_metric:
  note: "Editorial decision, không phải technical truth duy nhất."
```

Không con số nào sai. Nhưng đếm bằng `grep -c "def test_"` sẽ ra một con số thứ
ba nữa, vì collector loại trừ những thứ grep không biết. **Collector là định
nghĩa** — đừng đếm bằng tay rồi tưởng đã kiểm chứng.

### 6.6 Số liệu trong `docs/09-analysis/` nằm ngoài cơ chế này

Các tài liệu phân tích (`JD_MARKET_ANALYSIS.md`, `REFERENCE_DATASET_ANALYSIS.md`,
…) đo một lần trên corpus có MD5, ghi kèm ngày đo, và **không** có binding nào.
Đừng trích số từ đó vào README mà không kèm ngày và nguồn.

---

## 7. Quy trình thường dùng

### Đo lại sau khi đổi code

```bash
py -3 -m pytest -q -m "not integration"
```

```bash
git status --porcelain
```

Cả hai phải sạch, rồi:

```bash
py -3 scripts/generate_metrics_manifest.py --cob-dt 2026-09-22 --scope full
```

### Khi ra `status: warning` kèm README drift

```text
1. sửa số trong README
2. sửa `declared` + `readme_claim` trong manifest   ← contract text, KHÔNG phải giá trị đo
3. py -3 scripts/verify_readme_metrics.py           → phải 22/22
4. commit
5. sinh lại manifest từ worktree sạch               → status: verified
```

Bước 5 bắt buộc: `verification.status` và danh sách `warnings` được đóng dấu lúc
sinh. Sửa README xong mà không sinh lại thì manifest vẫn mang cảnh báo cũ.

### Kiểm nhanh, không cần Trino

```bash
py -3 scripts/generate_metrics_manifest.py --validate-contract
```

```bash
py -3 scripts/generate_metrics_manifest.py --render-sql --cob-dt 2026-09-22
```

`--validate-contract` kiểm contract tự nhất quán: mọi `metric` của invariant
phải khớp một node có thật, mọi `manifest_path` của binding phải tồn tại, mọi
projection phải có `claim` và `location`.

---

## 8. Trạng thái hiện tại

```text
manifest    status verified · 0 error · 0 warning · 0 skipped
            cob_dt 2026-09-22 · scope full · git_dirty false
metric      24 static · 11 runtime · 1 manual
invariant   23 (22 error · 1 warn) · toàn bộ operator eq
binding     18 entry → 22 projection · 22/22 khớp README
artifact    54 run artifact (không track)
promote     2026-09-27 · test_functions 810 · collected_pytest_nodes 1699
```
