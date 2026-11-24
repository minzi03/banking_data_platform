"""Create/check the local OpenMetadata RSA keypair used for JWT signing.

The private key is written only under gitignored ``docker/secrets/openmetadata``.
The script never prints key material. Existing valid pairs are preserved unless
``--rotate`` is explicitly requested; a mismatched/incomplete pair is rejected.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SECRETS_DIR = REPO_ROOT / "docker" / "secrets" / "openmetadata"
PRIVATE_KEY = SECRETS_DIR / "private_key.der"
PUBLIC_KEY = SECRETS_DIR / "public_key.der"


def _run_openssl(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["openssl", *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


def _derived_public_key(private_key: Path) -> bytes:
    # Use a temporary public-key file because OpenSSL binary stdout handling
    # differs across Windows and POSIX.
    with tempfile.TemporaryDirectory() as temp_dir:
        output = Path(temp_dir) / "public.der"
        result = _run_openssl(
            ["pkey", "-inform", "DER", "-in", str(private_key), "-pubout", "-outform", "DER", "-out", str(output)]
        )
        if result.returncode or not output.is_file():
            raise RuntimeError("OpenSSL could not derive the OpenMetadata public key")
        return output.read_bytes()


def check_keypair(private_key: Path = PRIVATE_KEY, public_key: Path = PUBLIC_KEY) -> list[str]:
    problems = []
    if not private_key.is_file():
        problems.append("OpenMetadata private key is missing")
    if not public_key.is_file():
        problems.append("OpenMetadata public key is missing")
    if problems:
        return problems
    try:
        derived = _derived_public_key(private_key)
    except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
        return [str(exc)]
    if derived != public_key.read_bytes():
        problems.append("OpenMetadata public key does not match the private key")
    return problems


def _set_private_permissions(path: Path) -> None:
    if os.name != "nt":
        path.chmod(0o600)


def generate_keypair(private_key: Path = PRIVATE_KEY, public_key: Path = PUBLIC_KEY) -> None:
    private_key.parent.mkdir(parents=True, exist_ok=True)
    public_key.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=private_key.parent) as temp_dir:
        temp = Path(temp_dir)
        private_pem = temp / "private.pem"
        private_der = temp / "private_key.der"
        public_der = temp / "public_key.der"
        generated = _run_openssl(
            ["genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", str(private_pem)]
        )
        if generated.returncode:
            raise RuntimeError("OpenSSL failed to generate the OpenMetadata RSA keypair")
        for args in (
            ["pkcs8", "-topk8", "-nocrypt", "-in", str(private_pem), "-outform", "DER", "-out", str(private_der)],
            ["pkey", "-in", str(private_pem), "-pubout", "-outform", "DER", "-out", str(public_der)],
        ):
            result = _run_openssl(args)
            if result.returncode:
                raise RuntimeError("OpenSSL failed to encode the OpenMetadata RSA keypair")
        _set_private_permissions(private_der)
        private_key.parent.mkdir(parents=True, exist_ok=True)
        public_key.parent.mkdir(parents=True, exist_ok=True)
        private_key.write_bytes(private_der.read_bytes())
        public_key.write_bytes(public_der.read_bytes())
    _set_private_permissions(private_key)
    problems = check_keypair(private_key, public_key)
    if problems:
        raise RuntimeError("Generated OpenMetadata keypair failed validation")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify the existing keypair without changing it")
    parser.add_argument("--rotate", action="store_true", help="replace an existing keypair explicitly")
    args = parser.parse_args(argv)

    if args.check:
        problems = check_keypair()
        for problem in problems:
            print(f"  FAIL: {problem}", file=sys.stderr)
        if not problems:
            print("OpenMetadata keypair: OK")
        return 1 if problems else 0

    existing = PRIVATE_KEY.exists() or PUBLIC_KEY.exists()
    if existing and not args.rotate:
        problems = check_keypair(PRIVATE_KEY, PUBLIC_KEY)
        if problems:
            for problem in problems:
                print(f"  FAIL: {problem}", file=sys.stderr)
            print("Refusing to replace key material implicitly; inspect it or pass --rotate.", file=sys.stderr)
            return 1
        print("OpenMetadata keypair already exists and is valid; unchanged.")
        return 0

    try:
        generate_keypair(PRIVATE_KEY, PUBLIC_KEY)
    except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
        print(f"OpenMetadata keypair generation failed: {exc}", file=sys.stderr)
        return 1
    location = SECRETS_DIR.relative_to(REPO_ROOT).as_posix()
    print(f"OpenMetadata RSA keypair created under {location}; private material not displayed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
