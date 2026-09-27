# ADR-0016 — Trino xác thực bằng mật khẩu qua HTTPS, mỗi client một credential

**Status**: Accepted — triển khai hai bước (PR-A: client và bootstrap; PR-B: bật trên server)
**Ngày**: 2026-09-27
**Liên quan**: [`0015`](0015-trino-access-control-generated-from-rbac.md) · [`RBAC_MATRIX.md`](../../06-security-compliance/RBAC_MATRIX.md) §5 · TD-3 · TD-15

---

## Context

ADR-0015 làm Trino **thực thi** quyền: mỗi client một user, tầng tiêu thụ chỉ đọc
`serving`, PII ở Bronze/Silver bị che. Nhưng nó ghi rõ giới hạn: Trino tin tên
user client tự khai trong header `X-Trino-User`. Ai kết nối được cổng 8080/8085
đều khai được `admin` và đọc CCCD nguyên bản. Luật phân quyền đúng, nhưng danh
tính thì không được kiểm.

Trước khi quyết định, đã đo trên một container `trinodb/trino:443` tách biệt
(`--network none`, không đụng stack) với `rules.json` thật của repo — không suy
từ tài liệu:

```text
cấu hình: HTTPS 8443 (keystore PKCS12 tự ký) + http-server.authentication.type=PASSWORD
          + password file + internal-communication.shared-secret

HTTP 8080, không mật khẩu            403 Forbidden
HTTP 8080, CÓ mật khẩu               403 Forbidden   — mật khẩu không bao giờ đi qua HTTP
HTTPS 8443, không mật khẩu           401 Unauthorized
HTTPS 8443, sai mật khẩu             401 Invalid credentials
HTTPS 8443, đúng mật khẩu            200, current_user = user đã đăng nhập
superset đăng nhập, --session-user admin
                                     "User superset cannot impersonate user admin"
trino CLI: --server https://… --insecure --user X --password, mật khẩu từ $TRINO_PASSWORD
                                     chạy, current_user = X
```

Hai điều quyết định thiết kế đến từ số đo:

1. **Bật password auth là cổng HTTP tự đóng với client.** Không cần tắt HTTP (nó
   vẫn dùng cho giao tiếp nội bộ, ký bằng shared secret).
2. **`rules.json` hiện có không có mục `impersonation`, và Trino coi đó là cấm mạo
   danh.** Danh tính đăng nhập chính là danh tính phân quyền — mọi dòng của
   RBAC_MATRIX có hiệu lực mà không phải sửa luật.

## Decision

**1. Password file authenticator, qua HTTPS.** Không LDAP/OAuth: stack chạy cục bộ,
không có identity provider. File mật khẩu là cơ chế Trino hỗ trợ sẵn, không thêm
service nào.

**2. Hash PBKDF2-HMAC-SHA1, 1.300.000 vòng**, định dạng `user:iterations:salt_hex:hash_hex`.
Đo trên spike: Trino 443 nhận PBKDF2 **SHA-1**; SHA-256 bị từ chối dù cùng định
dạng. Chọn PBKDF2 thay bcrypt vì sinh được bằng `hashlib` chuẩn — bcrypt cần
`htpasswd` từ image Docker Hub hoặc gói Python thêm; TD-15 vừa cho thấy phụ thuộc
image ngoài là rủi ro thật. 1.300.000 là mức OWASP cho PBKDF2-SHA1; đo được mỗi
lần gọi CLI ~1,1–1,4s ở cả 210k lẫn 1,3M vòng — chi phí nằm ở JVM của CLI, không
ở hash.

**3. Secret sinh lúc dựng stack, không bao giờ commit.** `scripts/bootstrap_trino_auth.py`
sinh vào `docker/secrets/trino/` (đã gitignore): keystore PKCS12 tự ký, cert PEM
cho client verify, file mật khẩu, và một mật khẩu ngẫu nhiên cho **mỗi user trong
`governance/rbac.py`**. Mật khẩu client đi vào `docker/.env` (đã gitignore) dưới
dạng `TRINO_PASSWORD_<USER>`. Chạy lại không đổi mật khẩu đã có, trừ khi `--rotate`.

**4. Client xác thực khi có `TRINO_PASSWORD`, chạy như cũ khi không có.** Mọi client
Python dùng một cách kết nối: có `TRINO_PASSWORD` → `https` + `BasicAuthentication`
+ verify bằng cert PEM; không có → HTTP như hiện tại. Nhờ vậy PR-A không đổi hành
vi, và PR-B chỉ còn là bật server + truyền biến môi trường.

**5. Danh sách user của file mật khẩu sinh từ `rbac.py`**, giống `rules.json`
(ADR-0015): một nguồn sự thật, không ai có mật khẩu mà không có role.

## Consequences

**Được**
- Khai `admin` không còn cho quyền admin. Có mật khẩu của `superset` cũng không mạo
  danh được `admin` (đo được, không suy).
- Mật khẩu không đi qua HTTP ở bất kỳ đâu.
- Không thêm service, không thêm image, không thêm gói Python.

**Mất**
- **Cert tự ký.** Client phải được đưa cert PEM để verify; CLI trong container dùng
  `--insecure` vì chỉ nói với `localhost` trong chính container. Không thay được PKI
  thật khi mang sang môi trường khác.
- **Đổi khoảng 12 client cùng lúc ở PR-B** — mọi lệnh `docker exec … trino` trong CI,
  benchmark, RUNBOOK phải mang credential.
- **Spark và MinIO vẫn đi vòng qua Trino.** ADR này chỉ khép lỗ hổng *danh tính trên
  Trino*. RBAC_MATRIX §5 mục 2–3 vẫn nguyên.
- **Mật khẩu nằm trong `docker/.env`** — bảo vệ bằng quyền file của máy dev, không
  bằng secret manager.
