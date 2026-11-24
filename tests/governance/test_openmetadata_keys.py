"""Tests for local-only OpenMetadata JWT key bootstrap and Compose wiring."""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "bootstrap_openmetadata_keys.py"
SPEC = importlib.util.spec_from_file_location("bootstrap_openmetadata_keys", SCRIPT)
bootstrap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bootstrap)


def test_bootstrap_generates_and_checks_matching_keypair(tmp_path):
    private_key = tmp_path / "openmetadata" / "private_key.der"
    public_key = tmp_path / "openmetadata" / "public_key.der"
    bootstrap.generate_keypair(private_key, public_key)
    assert private_key.is_file()
    assert public_key.is_file()
    assert bootstrap.check_keypair(private_key, public_key) == []
    with private_key.open("rb") as key_file:
        assert key_file.read(1) == b"\x30"  # DER-encoded PKCS#8 starts with an ASN.1 SEQUENCE.

    public_key.write_bytes(b"different public key")
    assert "OpenMetadata public key does not match the private key" in bootstrap.check_keypair(private_key, public_key)


def test_bootstrap_cli_creates_only_in_gitignored_secret_path(tmp_path, monkeypatch, capsys):
    secret_dir = tmp_path / "docker" / "secrets" / "openmetadata"
    monkeypatch.setattr(bootstrap, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(bootstrap, "SECRETS_DIR", secret_dir)
    monkeypatch.setattr(bootstrap, "PRIVATE_KEY", secret_dir / "private_key.der")
    monkeypatch.setattr(bootstrap, "PUBLIC_KEY", secret_dir / "public_key.der")
    assert bootstrap.main([]) == 0
    output = capsys.readouterr().out
    assert "private material not displayed" in output
    assert secret_dir.joinpath("private_key.der").exists()
    assert bootstrap.check_keypair() == []


def test_compose_mounts_secret_pair_for_both_openmetadata_services():
    compose = yaml.safe_load((REPO_ROOT / "docker" / "docker-compose.yml").read_text(encoding="utf-8"))
    for service_name in ("om-migrate", "openmetadata"):
        service = compose["services"][service_name]
        environment = service["environment"]
        assert environment["RSA_PRIVATE_KEY_FILE_PATH"] == "/run/secrets/openmetadata/private_key.der"
        assert environment["RSA_PUBLIC_KEY_FILE_PATH"] == "/run/secrets/openmetadata/public_key.der"
        volumes = service.get("volumes", [])
        assert "./secrets/openmetadata:/run/secrets/openmetadata:ro" in volumes


def test_openmetadata_private_key_file_is_gitignored():
    result = subprocess.run(
        ["git", "check-ignore", "docker/secrets/openmetadata/private_key.der"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, "OpenMetadata private key path must be ignored by Git"
