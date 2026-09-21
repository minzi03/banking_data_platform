# ADR-0012 — Tham số hoá generator bằng `--scale`, không viết seeder thứ hai

**Status**: Accepted
**Ngày**: 2026-09-14 (TD-6) · chuyển thể thành ADR 2026-09-22
**Liên quan**: [`../technical-debt.md`](../technical-debt.md) TD-6

---

## Context

Hai workflow cần cùng một lakehouse fixture nhưng với khối lượng khác nhau:

| Consumer | Cần gì |
|---|---|
| `benchmark.yml` | Khối lượng gần production — để đo hiệu năng có ý nghĩa |
| `ci.yml` job `trino-integration` | 34 test kiểm cấu trúc, tính duy nhất, toàn vẹn tham chiếu, khớp số lượng — **không cần 1,2 triệu giao dịch** |

Job integration tốn ~18 phút, trong đó **~13 phút là seeding và ETL**, còn bản thân test chỉ ~40 giây.

Cám dỗ rõ ràng: viết một "mini seeder" nhỏ gọn riêng cho CI.

## Decision

**Không viết seeder thứ hai.** Thay vào đó tham số hoá generator đang có bằng cờ `--scale`.

Lý do nằm ở một bài học đã trả giá, ghi trong TD-6:

> The failure mode to avoid is a separate small seeder: that is how the schemas in `trino-init` and `benchmark.yml` drifted apart in the first place.

Hai bản cài đặt sinh dữ liệu sẽ **phân kỳ**. Không phải có thể — mà chắc chắn, vì mỗi lần thêm một cột chỉ có một bên được sửa. Và phân kỳ kiểu đó không làm gì đỏ: CI vẫn xanh trên schema cũ của nó, trong khi production dùng schema mới.

Tiêu chí chấp nhận (TD-6):

```text
[ ] một generator/config canonical duy nhất điều khiển cả hai profile
      benchmark profile    → khối lượng gần production
      integration profile  → khối lượng giảm
[ ] cùng schema, cùng quy tắc sinh, cùng code path — CHỈ khác scale
[ ] thời gian chạy job integration giảm đáng kể
[ ] KHÔNG có bản "mini seed" thứ hai
```

## Consequences

**Được**

- Một nguồn sự thật cho schema và quy tắc sinh dữ liệu. Thêm cột là sửa một chỗ.
- CI chạy trên **cùng code path** với benchmark, nên lỗi sinh dữ liệu lộ ra ở cả hai.
- Không có đường nào để schema CI lặng lẽ tụt hậu.

**Mất**

- **Scale nhỏ làm lộ bug mà scale lớn giấu.** Đã xảy ra: `aml_customer_risk` sinh khoá chính trùng ở scale nhỏ, phải chuyển sang `random.sample`. Ở khối lượng lớn, va chạm ngẫu nhiên hiếm đến mức không ai thấy.
- Một số test phải bỏ giả định về khối lượng. Commit `a0113b2` gỡ các assertion dựa trên số dòng tuyệt đối vì chúng vỡ dưới `--scale`.
- Generator phải đúng ở **mọi** scale, không chỉ ở scale thiết kế. Đó là bề mặt kiểm thử rộng hơn.
- TD-6 vẫn ghi `deliberate`, không phải `fixed`: khoảng 250 dòng vẫn trùng lặp giữa `ci.yml` và `benchmark.yml`. Quyết định này giải quyết phần **sinh dữ liệu**, chưa giải quyết phần **orchestration**.

## Evidence

```text
docs/technical-debt.md TD-6            lý do đầy đủ + acceptance criteria
data_generator/                        generator canonical, cờ --scale
commit 8d9cc22                         perf: add --scale flag to seed generator for fast CI
commit 6cbb75d                         fix: random.sample cho aml_customer_risk PK ở scale nhỏ
commit a0113b2                         test: gỡ giả định khối lượng vỡ dưới --scale
```

Ghi chú: ba commit trên cho thấy cái giá thật của quyết định. Tham số hoá không phải thay một số — nó buộc phải sửa hai lỗi tiềm ẩn mà scale lớn đang che.
