# Chiến Lược Kiểm Thử

> Cập nhật: 2026-09-22 · Bản đồ tài liệu: [`../INDEX.md`](../INDEX.md)
> Liên quan: [`EVIDENCE_MANIFEST.md`](EVIDENCE_MANIFEST.md) · [`technical-debt.md`](technical-debt.md)

Mọi con số dưới đây đo trực tiếp trên repo tại `2026-09-22`, bằng
`pytest --collect-only` và `--cov`, không chép lại từ tài liệu khác. Chỗ nào
chưa đo được thì nói là chưa đo.

---

## 1. Năm tầng kiểm chứng, không phải một

pytest chỉ là một trong năm tầng. Trộn chúng vào nhau là cách người ta tưởng
"test xanh" nghĩa là "dữ liệu đúng".

| Tầng | Số lượng | Chạy khi nào | Trả lời câu hỏi gì |
|---|---:|---|---|
| **pytest, không hạ tầng** | 822 node | mọi lần push | Code và **cấu hình** có tự nhất quán? |
| **pytest integration** | 66 node | có cổng chặn | Chạy trên Spark/Trino thật có đúng? |
| **dbt test** | 110 generic + 7 singular | sau `dbt build` | Tầng serving có thoả hợp đồng? |
| **DQ check lúc chạy ETL** | 9 loại | mỗi job | Dữ liệu **lô này** có dùng được? |
| **Invariant của evidence manifest** | 22 | sau full rebuild | Số liệu công bố có đúng? |

Không tầng nào thay được tầng khác. Ví dụ cụ thể: pytest xanh 100% vẫn không
nói gì về việc Gold có bị fan-out trên dữ liệu thật — đó là việc của tầng 2 và
tầng 5. Ngược lại, manifest `verified` không nói gì về việc code có xử lý đúng
trường hợp biên — đó là tầng 1.

Con số dbt đo bằng cách parse `_serving_models.yml` (98) và `_gold_sources.yml`
(12) rồi cộng 7 file singular test: **117**. Lần `dbt build` sau re-seed báo
`PASS=117`. Hai phép đo độc lập trùng nhau, nên con số này đáng tin — khác với
suy luận bằng `grep`, vốn cho 113 vì đếm cả chuỗi ngoài khối `tests:`.

---

## 2. Biên giới là marker, không phải thư mục

Chỉ có **một** marker thực sự chia suite: `integration`.

```bash
py -3 -m pytest -q -m "not integration"
```

```text
821 passed, 1 skipped, 66 deselected  ·  14s
```

66 test `integration` **không** nằm gọn trong `tests/integration/`:

| File | Node | Cần gì |
|---|---:|---|
| `tests/integration/test_data_quality.py` | 18 | Docker + Trino |
| `tests/integration/test_etl_validation.py` | 16 | Docker + Trino |
| `tests/gold/test_gold_fanout_regression.py` | 23 | pyspark cục bộ |
| `tests/gold/test_business_date_semantics.py` | 9 | pyspark cục bộ |

Nghĩa là **32 trong 66** test integration sống ngoài `tests/integration/`. Đừng
dùng đường dẫn để chọn tập test; dùng marker.

### Hai chỗ tên thư mục nói không đúng

**`tests/integration/test_governance_e2e.py` (8 node) KHÔNG mang marker
`integration`** — nên nó chạy trong suite mặc định. Không phải lỗi: nó dùng
`tmp_path` và `MagicMock`, không cần hạ tầng nào. "Integration" ở đây nghĩa là
*tích hợp giữa các module governance*, không phải tích hợp hạ tầng. Nhưng thư
mục thì không nói được sự phân biệt đó.

**`tests/bronze/` chỉ có `__init__.py`** — không một test nào. Bronze được phủ
gián tiếp qua `tests/governance/test_declared_sources_match_sql.py` (hợp đồng
YAML) và qua integration test. Thư mục rỗng là lời hứa chưa thực hiện.

---

## 3. Suite hiện tại, đo được

| Thư mục | Hàm `def test_*` | Node sau parametrize |
|---|---:|---:|
| `governance/` | 297 | 464 |
| `gold/` | 124 | 144 |
| `ops/` | 86 | 88 |
| `shared/` | 49 | 51 |
| `integration/` | 42 | 42 |
| `plugins/` | 25 | 25 |
| `data_generator/` | 21 | 63 |
| `silver/` | 11 | 11 |
| `bronze/` | 0 | 0 |
| **Tổng** | **655** | **888** |

`governance/` chiếm **52%** số node. Đó là lựa chọn có chủ ý, xem §5.

Test bị skip duy nhất là `test_metrics_manifest_contract.py:109` — *"manifest đã
được sinh, kiểm tra này chỉ áp cho skeleton"*. Nó cố tình skip khi manifest đã
thật, nên skip ở đây là trạng thái ĐÚNG, không phải nợ.

### 655 và 888 đều đúng

```yaml
test_functions:          value: 655   # def test_*
collected_pytest_nodes:  value: 888   # sau khi parametrize nở ra
```

Chênh 233 là do parametrize. Rõ nhất ở `data_generator/`: 21 hàm → 63 node.

**Ví dụ đo được ngay trong commit này**: `test_docs_links_resolve.py`
parametrize theo danh sách markdown lấy từ `git ls-files`, nên **chính tài liệu
bạn đang đọc** làm `collected_pytest_nodes` đi từ 887 lên 888, còn
`test_functions` giữ nguyên 655. Trước đó `EVIDENCE_MANIFEST.md` đã làm y hệt:
886 → 887.

Chi tiết đáng chú ý: nguồn là `git ls-files`, **không** phải glob trên đĩa. Nên
số node chỉ tăng sau khi file được stage hoặc commit — viết file ra rồi chạy
pytest ngay thì vẫn thấy con số cũ. Tôi đã nhầm đúng chỗ này lúc soạn tài liệu:
đếm được 887 trên file chưa track và tưởng cơ chế parametrize không áp cho nó.

Hệ quả: **đừng đếm test bằng tay.** `grep -c "def test_"` cho con số thứ ba nữa
vì nó không biết collector loại trừ gì. Collector là định nghĩa —
[`EVIDENCE_MANIFEST.md` §6.5](EVIDENCE_MANIFEST.md).

---

## 4. Ba job CI chạy test

| Job | Tập test | Cổng chặn |
|---|---|---|
| 🧪 **Unit Tests + SQL Invariants** | `-m "not integration"` + coverage + `verify_readme_metrics.py` | không — luôn chạy |
| 🔬 **Gold Spark Regression** | 32 test (23 fan-out + 9 business-date), pyspark thật | đổi `code_etl/gold/`, `code_etl/shared/spark/`, `tests/gold/`, `docker/spark/conf/`, `.github/workflows/` |
| 🧪 **Trino Integration** | 34 test, dựng cả stack Docker | đổi `code_etl/`, `data_generator/`, `docker/`, `tests/integration/`, `pyproject.toml`, … |

Cổng của Gold Spark Regression liệt `code_etl/shared/spark/` và
`docker/spark/conf/` **có lý do ghi trong workflow**:
`test_business_date_semantics.py` assert trực tiếp lên `assert_utc_session()` và
lên dòng `spark.sql.session.timeZone=UTC`. Không liệt thì một PR chỉ sửa
timezone sẽ bỏ qua đúng job canh nó.

Job Unit Tests cũng là nơi chạy `verify_readme_metrics.py` — README drift là
lỗi build, không phải việc review bằng mắt.

> **TD-1 vẫn mở**: Trino Integration bị cổng theo đường dẫn, nên phần lớn PR
> không chạy 34 test đó. Xem [`technical-debt.md`](technical-debt.md).

---

## 5. Vì sao `governance/` chiếm một nửa suite

Vì phần lớn thứ có thể hỏng ở nền tảng dữ liệu **không phải logic Python**. Nó
là cấu hình, hợp đồng, và tài liệu trôi khỏi thực tế. Những test này assert lên
đúng chỗ đó:

| File | Node | Bắt điều gì |
|---|---:|---|
| `test_docs_links_resolve.py` | 59 | link chết trong tài liệu |
| `test_generate_metrics_manifest.py` | 57 | chính cái verifier có đúng không |
| `test_rbac.py` | 54 | ma trận quyền |
| `test_declared_sources_match_sql.py` | 49 | **YAML khai nguồn nào thì SQL phải đọc đúng nguồn đó** |
| `test_airflow_dag_contracts.py` | 44 | DAG khớp job config |
| `test_docs_no_stale_claims.py` | 24 | số liệu cũ quay lại tài liệu |

`test_declared_sources_match_sql.py` đáng chú ý nhất vì nó bắt một lớp lỗi mà
test hành vi không thể thấy: `customer_360.yml` từng khai
`fact_online_transaction` ở ba chỗ trong khi SQL không hề đọc bảng đó. Guard
`require_snapshots` vì thế chặn job vì một bảng không liên quan — một **false
failure** mà mọi test logic vẫn xanh. Quyết định nền: [ADR-0013](../02-architecture/adr/0013-declared-sources-match-sql.md).

---

## 6. Test phải chứng minh guard BẮT được lỗi

Một test khẳng định "guard tồn tại" gần như vô giá trị. Test phải dựng đúng
trạng thái lỗi rồi chứng minh guard phát hiện, **và** chứng minh guard yếu hơn
thì không phát hiện.

Ví dụ mẫu trong repo — `tests/gold/test_gold_fanout_regression.py:428`:

```python
def test_missing_snapshot_would_NOT_be_caught_by_non_empty_alone(self, silver_tables, spark):
```

Đây là **negative control**. Nó dựng tình huống thiếu snapshot và chứng minh
`assert_non_empty()` một mình *không* bắt được — tức chứng minh
`assert_source_snapshots()` không phải guard thừa. Không có test này thì không
ai biết hai guard có trùng vai hay không.

Cùng tinh thần đó ở nhiều chỗ khác:

- `test_naive_cast_is_wrong_at_boundary` — chứng minh cách làm sai thật sự sai,
  chứ không chỉ chứng minh cách đúng thì đúng
- `test_card_summary_amount_not_multiplied_by_card_count` — đặt tên theo **lỗi
  từng xảy ra**, không theo hàm được test
- `test_high_value_threshold_flags_about_one_percent` — assert lên **hình dạng
  phân phối**, không chỉ lên việc sampler trả về số

Tên test nên gọi tên lỗi mà nó chặn. `test_gold_job` không cho biết gì; khi nó
đỏ, không ai biết cái gì vỡ.

### Test parametrize phải tự canh việc nó có kiểm gì không

Một test parametrize trên danh sách rỗng sẽ **xanh tuyệt đối**. pytest báo 0
node, không ai để ý. `test_docs_links_resolve.py` chặn điều đó bằng hai guard
riêng, đứng ngoài phần parametrize:

```python
def test_markdown_files_are_found():
    assert len(MARKDOWN_FILES) >= 30    # git ls-files hỏng → mọi test dưới xanh vô nghĩa

def test_internal_links_are_found():
    assert total >= 100                 # regex trích link hỏng → cũng vậy
```

Bất kỳ test nào lấy tập đầu vào từ hệ thống file, `git`, hay một query đều cần
cặp guard này. Nguồn dữ liệu hỏng và tập rỗng là **cùng một** biểu hiện với "mọi
thứ đều đúng".

> Các mẫu false-success đã gặp (đọc `$?` sau pipe, `set -e` không lan vào
> subshell, `grep` trên output rỗng) được ghi ở **TD-5**, đã đóng, và được canh
> bởi `tests/governance/test_shell_failure_propagation.py`.

---

## 7. Coverage: đo gì, và KHÔNG đo gì

CI đo coverage trên **hai** package:

```bash
py -3 -m pytest tests/ -m "not integration" --cov=governance --cov=code_etl/shared --cov-report=term
```

```text
TOTAL  1464 statement · 387 miss · 74%
Required test coverage of 60.0% reached. Total coverage: 73.57%
```

Ngưỡng `fail_under = 60` trong `pyproject.toml` **có hiệu lực** — dòng
"Required test coverage… reached" là do nó.

Thấp nhất trong phạm vi đã đo:

| Module | Coverage |
|---|---:|
| `code_etl/shared/ops/lineage_tracker.py` | 0% |
| `code_etl/shared/spark/iceberg_utils.py` | 17% |
| `code_etl/shared/ops/data_quality.py` | 53% |
| `code_etl/shared/ops/iceberg_maintenance.py` | 58% |

### Cấu hình và CI đang nói hai chuyện khác nhau

```toml
# pyproject.toml
[tool.coverage.run]
source = ["code_etl", "data_generator", "api", "ml"]
```

CI truyền `--cov=governance --cov=code_etl/shared`, ghi đè hoàn toàn. Kết quả:

- `governance/` **không** có trong config nhưng lại là thứ được đo nhiều nhất
- `data_generator/`, `api/`, `ml/` có trong config nhưng **chưa bao giờ** được đo
- `code_etl/bronze/`, `silver/`, `gold/`, `cdc/` và `scripts/` không được đo

`api/main.py`, `ml/pipeline/churn_prediction.py`, `ml/pipeline/credit_scoring.py`,
`ml/monitoring/drift_detection.py` tồn tại và **không có test nào import
chúng**. Con số 74% chỉ nói về `governance` + `code_etl/shared`; nó không phải
coverage của dự án.

---

## 8. Cấu hình pytest nằm đúng một chỗ

`[tool.pytest.ini_options]` trong `pyproject.toml` là **nguồn duy nhất**.
`pytest.ini` đã gỡ. Kiểm bằng dòng pytest tự in:

```text
configfile: pyproject.toml
```

Không còn `(WARNING: ignoring pytest config in pyproject.toml!)`.

**Vì sao phải hợp nhất, không phải để cho gọn.** pytest chọn **một** configfile
theo thứ tự ưu tiên (`pytest.ini` > `pyproject.toml`) rồi **bỏ hoàn toàn** phần
còn lại — nó không merge hai file. Khi cả hai cùng tồn tại, ba thứ hỏng:

1. Marker `security` chỉ khai trong `pyproject.toml` → **không bao giờ được
   đăng ký**. `@pytest.mark.security` là marker lạ, không lọc được gì.
2. Marker `slow` được đăng ký (qua `pytest.ini`) nhưng **dùng 0 lần** — cấu
   hình chết.
3. Hai nơi khai marker trôi khỏi nhau, vì không gì so chúng.

Đó là lý do đừng tạo lại `pytest.ini`, `setup.cfg` hay `tox.ini`: thêm file có
độ ưu tiên cao hơn sẽ **âm thầm** vô hiệu cả block trong `pyproject.toml`, và
triệu chứng duy nhất là một dòng banner ít ai đọc.

**Marker sau khi hợp nhất — còn đúng một cái:**

| Marker | Dùng | Quyết định |
|---|---|---|
| `integration` | 66 test | giữ — đây là ranh giới suite thật (§4, §10) |
| `slow` | 0 test | **xoá** — không test nào mang, không job CI nào lọc theo nó |
| `security` | 0 test | **xoá** — chưa từng được đăng ký nên chưa từng dùng được |

`py -3 -m pytest --markers` giờ liệt đúng `integration` (cộng các marker built-in
của pytest và plugin).

`slow` bị xoá chứ không được gán cho 32 test pyspark ở `tests/gold/`: nhóm đó đã
mang `integration` và đã bị loại khỏi suite mặc định, nên `slow` không thêm sức
lọc nào. Nếu sau này cần tách "chậm nhưng không cần hạ tầng", khai lại marker
**cùng lúc** với test đầu tiên mang nó — đừng khai trước.

`addopts` giữ nguyên `-v --tb=short`. Bản trong `pyproject.toml` từng có thêm
`--strict-markers` nhưng chưa bao giờ chạy (cả block bị bỏ qua); không bật kèm
ở đây để thay đổi này chỉ là hợp nhất, không đổi hành vi. Bật `--strict-markers`
là việc riêng, và giờ mới an toàn vì marker đã về một chỗ.

`[tool.coverage.*]` **không** liên quan: coverage.py đọc `pyproject.toml` trực
tiếp, không qua cơ chế configfile của pytest, nên nó chưa bao giờ bị bỏ qua.
Vấn đề của nó là lệch scope — xem §7.

---

## 9. 34 test Trino không chạy được ở local

`tests/integration/test_data_quality.py` và `test_etl_validation.py` gọi Trino
qua tên container **hardcode**:

```python
cmd = ["docker", "exec", "ci-trino", "trino", ...]
```

`ci-trino` là tên trong `docker-compose.ci.yml`. Stack local đặt tên
`banking-trino` (`docker/docker-compose.yml:421`). Nên chạy nhóm test này trên
stack local sẽ fail ở `docker exec`, không phải ở assertion.

Nghĩa là 34 test đó **chỉ** chạy được trong CI. Không có cách verify cục bộ
trước khi push — cộng với cổng chặn ở §4, đây là lý do TD-1 khó đóng.

---

## 10. Cách chạy

Suite mặc định, không cần hạ tầng gì:

```bash
py -3 -m pytest -q -m "not integration"
```

Một nhóm, có tên test:

```bash
py -3 -m pytest tests/governance/test_declared_sources_match_sql.py -v
```

Nhóm Gold cần pyspark thật (Java 17 + `pyspark==3.5.3`):

```bash
py -3 -m pytest tests/gold/test_gold_fanout_regression.py tests/gold/test_business_date_semantics.py -v -m integration
```

Coverage giống CI:

```bash
py -3 -m pytest tests/ -q -m "not integration" --cov=governance --cov=code_etl/shared --cov-report=term-missing
```

Xem test nào bị skip và vì sao:

```bash
py -3 -m pytest -q -m "not integration" -rs
```

**Console Windows**: đặt `PYTHONIOENCODING=utf-8` trước lệnh, nếu không cp1258
sẽ ném `UnicodeEncodeError` trên thông báo tiếng Việt và giết tiến trình trước
khi in được lỗi thật.

---

## 11. Khi thêm test mới

1. **Đặt tên theo lỗi nó chặn**, không theo hàm nó gọi.
2. **Viết docstring nói lỗi đó đã xảy ra như thế nào.** Repo này dùng docstring
   để chở lý do; xem `test_amount_distribution.py` hoặc
   `test_docs_no_stale_claims.py`.
3. **Cần hạ tầng thì đánh `@pytest.mark.integration`** — kể cả khi file nằm
   ngoài `tests/integration/`.
4. **Chứng minh test đỏ được.** Sửa code cho sai rồi chạy lại. Một test chưa bao
   giờ đỏ chưa được kiểm chứng là có tác dụng.
5. **Thêm negative control** nếu đang test một guard: chứng minh guard yếu hơn
   không bắt được.
6. Nếu đổi số test trong tài liệu, **sinh lại manifest** —
   [`EVIDENCE_MANIFEST.md` §7](EVIDENCE_MANIFEST.md).

---

## 12. Chưa có

Ghi ra để không ai tưởng suite này phủ nhiều hơn thực tế.

| Thiếu | Ảnh hưởng |
|---|---|
| Test cho `api/` và `ml/` | 4 module Python không có test nào |
| Test đơn vị cho Bronze | `tests/bronze/` rỗng; chỉ phủ qua hợp đồng + integration |
| Test đơn vị cho SCD2 | `tests/silver/` chỉ có SCD Type 1; SCD2 chỉ phủ qua integration |
| Số dbt test trong manifest | manifest đếm `dbt_models = 13` nhưng không đếm dbt **test**; cả 117 test đó nằm ngoài evidence manifest, nên không gì chặn chúng biến mất |
| Property-based test | phân phối amount kiểm bằng mẫu seed cố định, không bằng sinh ngẫu nhiên có shrink |
| Coverage cho Bronze/Silver/Gold job | ngoài `--cov` của CI |
| Mutation testing | không có bằng chứng nào cho thấy test *bắt* được thay đổi, ngoài negative control viết tay |
| `--strict-markers` | marker gõ sai chỉ cảnh báo rồi chạy tiếp, không đỏ — §8 |
| Guard chặn `pytest.ini`/`setup.cfg`/`tox.ini` quay lại | không gì chặn ai đó thêm lại file ưu tiên cao hơn và vô hiệu cấu hình trong `pyproject.toml` — §8 |

---

## 13. Trạng thái hiện tại

```text
pytest      888 node · 655 hàm · 822 node chạy không cần hạ tầng
suite       821 passed · 1 skipped (có chủ ý) · 66 deselected · 14s
coverage    74% trên governance + code_etl/shared · fail_under 60 đang áp
marker      1 đăng ký (integration dùng 66 lần) · configfile: pyproject.toml
dbt         110 generic + 7 singular = 117 (khớp PASS=117 của dbt build)
DQ runtime  9 loại check (CHECK_DISPATCH)
manifest    22 invariant
```
