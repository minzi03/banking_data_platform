# Đóng Góp — Banking Data Platform

Bản đồ tài liệu: [`docs/INDEX.md`](docs/INDEX.md)

---

## Nguyên tắc

Dự án này ưu tiên **tính đúng đắn kiểm chứng được** hơn tốc độ giao hàng. Hai quy tắc chi phối mọi thay đổi:

> **1. Một test xanh chỉ có giá trị khi bản thân invariant là đúng.**
> Không bao giờ làm một gate xanh bằng cách nới lỏng điều kiện nó kiểm tra. Gate đỏ thì sửa nguyên nhân, hoặc ghi nhận thành nợ có tên trong [`docs/technical-debt.md`](docs/05-quality/technical-debt.md).

> **2. Một fallback trông có vẻ phòng thủ có thể là một phép đo bịa.**
> `|| true`, `|| echo 0`, đọc `$?` sau pipe — tất cả đều biến thất bại thành thành công giả. Xem TD-5 để biết pattern này đã xuất hiện 7 lần.

Hệ quả thực tế: **`not_collected ≠ verified`**. Nếu chưa đo được thì ghi `not_collected`, đừng viết như đã có.

---

## Chạy thử trước khi gửi

```bash
py -3 -m pytest -q -m "not integration"
```

```bash
py -3 -m ruff check governance/ code_etl/ tests/
```

```bash
py -3 -m ruff format --check governance/ code_etl/ tests/
```

Ba lệnh trên khớp với job `lint` và `test` trên CI. Cả ba phải sạch trước khi mở PR.

**Đọc mã thoát cho đúng.** Đừng nối `| tail`, `| grep`, hay `| head` rồi đọc `$?` — bạn sẽ nhận mã của lệnh cuối trong pipe, không phải của lệnh mình quan tâm. Trên Windows cũng đừng redirect vào `/tmp/...`: đường dẫn đó không ghi được và lỗi redirect sẽ bị nhầm thành lỗi của lệnh.

### Test integration

66 test cần Docker + Spark + Trino, bị loại khỏi lệnh trên bằng marker `integration`:

```bash
py -3 -m pytest -q -m integration
```

Chúng chạy trên CI trong job `Trino Integration`, và job `Trino Integration Gate` chặn PR khi thay đổi chạm vào ETL mà integration không chạy.

---

## Quy ước nhánh và commit

```text
fix/<mô-tả-ngắn>       sửa lỗi
feat/<mô-tả-ngắn>      tính năng mới
docs/<mô-tả-ngắn>      tài liệu
test/<mô-tả-ngắn>      chỉ test
```

Không commit thẳng lên `main`.

**Commit message**: dòng đầu là `<loại>: <việc đã làm>` ở thể mệnh lệnh. Phần thân giải thích **vì sao**, không phải **cái gì** — diff đã nói cái gì rồi. Nếu thay đổi sửa một lỗi, mô tả lỗi đó biểu hiện ra sao và tại sao nó lọt qua được.

---

## Định nghĩa "xong"

Một thay đổi được coi là xong khi:

- [ ] `pytest -m "not integration"` xanh, và **số test mới được nêu rõ trong commit message**
- [ ] `ruff check` và `ruff format --check` sạch
- [ ] Nếu sửa lỗi: có test **fail trước khi sửa, pass sau khi sửa**
- [ ] Nếu thêm invariant: có test negative chứng minh nó thực sự chặn được
- [ ] Nếu đổi số liệu công bố: regenerate manifest, không sửa tay
- [ ] Nếu thêm tài liệu: đã thêm vào [`docs/INDEX.md`](docs/INDEX.md)

---

## Thay đổi mô hình dữ liệu

Các job ở cả ba tầng đều **metadata-driven**: logic nằm trong YAML, engine dùng chung nằm trong `base_job/`.

Khi sửa một model, giữ ba thứ này đồng bộ:

```text
source.tables                  bảng nguồn — dùng cho lineage
upstream_flags                 bảng chờ — dùng cho SqlSensor
validation.require_snapshots   bảng CHẶN — assert_source_snapshots() fail nếu thiếu partition
```

`tests/governance/test_declared_sources_match_sql.py` bắt buộc mọi bảng khai báo phải thực sự xuất hiện trong `sql`. Khai báo thừa ở `require_snapshots` sẽ làm job chết vì thiếu partition của một bảng mà output không hề phụ thuộc — một lỗi giả.

**Thêm cột vào Gold**: cập nhật cả `docker/init_iceberg/03_ddl_gold.sql`. Tầng serving dùng `select *` là chủ ý nên tự bắt cột mới; data contract dùng `required_columns` dạng tập con nên không vỡ.

---

## Số liệu và tài liệu

Số liệu công bố chỉ sống ở một nơi: `docs/evidence/metrics-manifest.yaml`.

```bash
py -3 scripts/generate_metrics_manifest.py --cob-dt <YYYY-MM-DD> --scope full
```

Không dùng `--allow-dirty`. Manifest sinh từ worktree bẩn sẽ ghi `git_dirty: False` sai sự thật — tức là chính công cụ kiểm chứng lại nói dối.

`scripts/verify_readme_metrics.py` so README với manifest (18 binding). Lưu ý giới hạn: nó **không** so manifest với thực tế — vòng lặp chỉ khép khi regenerate.

Khi thêm tài liệu mới, thêm dòng tương ứng vào [`docs/INDEX.md`](docs/INDEX.md). Cấu trúc đích và lý do: [`docs/DOCUMENTATION_PLAN.md`](docs/09-analysis/DOCUMENTATION_PLAN.md).

---

## Bảo mật

Không commit secrets. Xem [`SECURITY.md`](SECURITY.md) trước khi đụng vào `docker/secrets/`, credential, hay dữ liệu có PII.
