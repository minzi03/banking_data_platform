"""
Terraform không commit secret (TD-3 nhóm D)
===========================================

`terraform/terraform.tfvars` từng được commit kèm mật khẩu Postgres và MinIO, và
`variables.tf` đặt chính các mật khẩu đó làm `default` — thiếu tfvars thì
`terraform apply` âm thầm dùng giá trị ai đọc repo cũng biết. `.gitignore` cũng
không bỏ qua `terraform.tfstate`, nơi Terraform lưu secret dạng rõ.

Nay: secret không có default, chỉ `terraform.tfvars.example` được commit, tfvars
thật và state bị gitignore. Bản tfvars cũ còn thiếu `alertmanager` trong `ports`
(object bắt buộc đủ thuộc tính) — `terraform plan` với nó hỏng từ trước, và CI
không chạy Terraform nên không ai thấy; test cuối khoá chỗ đó.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TF_DIR = REPO_ROOT / "terraform"
EXAMPLE = TF_DIR / "terraform.tfvars.example"
KNOWN_LITERALS = ("BankingAdmin123", "admin123", "Minioadmin123", "CDCPassword123")
_LITERAL_RE = re.compile(r"\b(" + "|".join(KNOWN_LITERALS) + r")\b")

# Khối `variable "x" { … }` kết thúc ở dòng `}` đầu cột.
_VARIABLE_RE = re.compile(r'^variable "(\w+)" \{\n(.*?)^\}', re.MULTILINE | re.DOTALL)
_SECRET_NAME = re.compile(r"password|secret", re.IGNORECASE)


def _variables(text: str) -> dict[str, str]:
    return dict(_VARIABLE_RE.findall(text))


VARIABLES = _variables((TF_DIR / "variables.tf").read_text(encoding="utf-8"))


def _object_keys(block: str) -> set[str]:
    return set(re.findall(r"^\s*(\w+)\s*=", block, re.MULTILINE))


def test_variables_are_parsed():
    assert {"postgres_password", "minio_secret_key", "ports"} <= set(VARIABLES)


def test_secret_variables_are_sensitive_and_have_no_default():
    wrong = []
    for name, body in VARIABLES.items():
        if not _SECRET_NAME.search(name):
            continue
        if not re.search(r"^\s*sensitive\s*=\s*true", body, re.MULTILINE):
            wrong.append(f"{name}: thiếu sensitive = true")
        if re.search(r"^\s*default\s*=", body, re.MULTILINE):
            wrong.append(f"{name}: có default — thiếu tfvars thì apply dùng giá trị đã commit")
    assert not wrong, "\n".join(wrong)


def test_no_known_secret_literal_in_committed_terraform():
    committed = sorted(TF_DIR.glob("*.tf")) + [EXAMPLE]
    hits = [
        f"{p.relative_to(REPO_ROOT).as_posix()}:{n}: chứa `{lit}`"
        for p in committed
        for n, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
        for lit in _LITERAL_RE.findall(line)
    ]
    assert not hits, "\n".join(hits)


def test_example_leaves_every_secret_as_a_placeholder():
    text = EXAMPLE.read_text(encoding="utf-8")
    secrets = [name for name in VARIABLES if _SECRET_NAME.search(name)]
    for name in secrets:
        m = re.search(rf'^{name}\s*=\s*"([^"]*)"', text, re.MULTILINE)
        assert m, f"{name} không có trong terraform.tfvars.example"
        assert m.group(1) == "CHANGE_ME", f"{name} trong .example phải là CHANGE_ME"


def test_real_tfvars_and_state_are_gitignored():
    for rel in (
        "terraform/terraform.tfvars",
        "terraform/prod.tfvars",
        "terraform/terraform.tfstate",
        "terraform/terraform.tfstate.backup",
        "terraform/.terraform/providers",
    ):
        out = subprocess.run(["git", "check-ignore", rel], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        assert out.returncode == 0, f"{rel} KHÔNG bị gitignore — secret có thể bị commit"
    out = subprocess.run(
        ["git", "check-ignore", "terraform/terraform.tfvars.example"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 1, "terraform.tfvars.example phải được commit"


def test_example_ports_match_the_declared_object_type():
    """`ports` là object: thiếu một thuộc tính thì terraform plan dừng lại."""
    body = VARIABLES["ports"]
    declared = _object_keys(re.search(r"type = object\(\{(.*?)\}\)", body, re.DOTALL).group(1))
    given = _object_keys(re.search(r"^ports = \{(.*?)^\}", EXAMPLE.read_text(encoding="utf-8"), re.M | re.S).group(1))
    assert declared == given, f"thiếu: {sorted(declared - given)} · thừa: {sorted(given - declared)}"
