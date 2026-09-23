# ADR-0008 — Evidence manifest là verifier, không phải nơi dump số

**Status**: Accepted
**Ngày**: 2026-09 · chuyển thể thành ADR 2026-09-22
**Liên quan**: [`0005`](0005-fail-loud-before-overwrite.md)

---

## Context

README công bố hàng chục con số: 17 nguồn, 14 Gold model, 2.300.000 giao dịch, 29 service. Mỗi con số là một tuyên bố có thể sai — và sai theo kiểu không ai phát hiện, vì tài liệu không chạy.

Cách thông thường là chép số vào README rồi thỉnh thoảng cập nhật tay. Cách đó hỏng theo thời gian một cách có hệ thống: code đổi, số ở README không đổi, và không có gì báo.

Cách thứ hai — sinh số tự động rồi ghi thẳng vào tài liệu — tốt hơn nhưng vẫn thiếu một thứ: nó không phân biệt được **"đo được và đúng"** với **"đo được"**.

## Decision

`scripts/generate_metrics_manifest.py` **không phải script dump số. Nó là verifier** với ba pha tách bạch:

```text
COLLECT  →  luôn ghi timestamped run artifact
   ↓
VERIFY   →  chạy blocking invariants
   ↓
ERROR == 0 ?  yes → promote canonical manifest
              no  → GIỮ canonical cũ, exit != 0
```

Điểm mấu chốt nằm ở nhánh `no`: khi invariant đỏ, manifest canonical **không bị cập nhật**. Nghĩa là tài liệu giữ nguyên trạng thái đã kiểm chứng lần cuối thay vì nhận số mới chưa qua kiểm.

Tách COLLECT khỏi VERIFY còn để lại **forensic evidence**: một lần rebuild fail vẫn ghi artifact vào `docs/evidence/generated/` thay vì mất trắng.

Ba khái niệm đi kèm:

| Khái niệm | Ý nghĩa |
|---|---|
| `declared` vs `value` | Số khai báo vs số đo được — lệch nhau là invariant fail |
| `metric_type` | `static` (đọc từ file) · `runtime` (đo qua Trino) · `manual` (đo một lần, có ngày) |
| `verification_scope` | `batch` · `cdc` · `full` — mỗi scope khai báo rõ nó bỏ qua gì và vì sao |

Và một quy tắc: **`not_collected ≠ verified`**. Metric chưa đo được ghi `not_collected`, không ghi 0, không ghi giá trị đoán.

README chiếu từ manifest qua 18 `readme_bindings`, kiểm bởi `scripts/verify_readme_metrics.py`.

## Consequences

**Được**

- Số trong README không thể lệch khỏi manifest mà CI không biết.
- Một rebuild fail không làm hỏng tài liệu đang đúng.
- `metric_type: manual` cho phép đưa số đo thủ công vào hệ thống **mà vẫn phân biệt được** với số đo tự động — thay vì cấm hẳn rồi để chúng trôi nổi trong tài liệu không ai kiểm.

**Mất**

- **Vòng lặp chỉ khép khi regenerate.** `verify_readme_metrics.py` so README ↔ manifest, **không** so manifest ↔ thực tế. Nếu manifest cũ, nó vẫn báo 22/22 xanh. Đây là giới hạn thật, không phải lỗi: hiện `test_functions` ghi 476 trong khi thực tế là 598.
- Thêm một bước bắt buộc vào mọi thay đổi ảnh hưởng số liệu.
- `--allow-dirty` tồn tại và **nguy hiểm**: nó khiến manifest ghi `git_dirty: False` không đúng sự thật, tức là chính công cụ kiểm chứng nói dối. Đã từng xảy ra.

  > **Ghi chú bổ sung (2026-09-23)** — mục này không còn đúng với code hiện tại; giữ nguyên văn vì ADR ghi lại trạng thái lúc quyết định.
  > - `0045922` sửa phần nói dối: `git_dirty` luôn ghi đúng sự thật, cờ chỉ còn là cổng promote.
  > - [#28](https://github.com/minzi03/banking_data_platform/pull/28) gỡ hẳn cờ: invariant `worktree_clean` (severity `error`) đã chặn cây bẩn trước, nên cờ không đổi hành vi trong trường hợp nào. Cây bẩn giờ chỉ chạy được với `--collect-only` (không promote).
  >
  > Chi tiết: [`EVIDENCE_MANIFEST.md` §6.2](../../05-quality/EVIDENCE_MANIFEST.md).
- Metric `manual` không tự già đi. Một con số đo tháng trước trông y hệt con số đo hôm nay nếu không đọc trường ngày.

## Evidence

```text
scripts/generate_metrics_manifest.py       docstring mô tả COLLECT → VERIFY → promote
scripts/verify_readme_metrics.py           18 readme_bindings
docs/evidence/metrics-manifest.yaml        40 metric node · 22 invariant
tests/governance/test_metrics_manifest_contract.py
tests/governance/test_readme_projections.py
```
