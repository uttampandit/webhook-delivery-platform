#!/usr/bin/env python3
"""Check local secret values against repository files and current Compose logs."""

import os
import subprocess
import sys
import tarfile
from io import BytesIO


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SECRET_KEYS = {
    "POSTGRES_PASSWORD",
    "API_TOKEN",
    "DEMO_CONTROL_TOKEN",
    "FULFILLMENT_SIGNING_KEY_V1",
    "NOTIFICATION_SIGNING_KEY_V1",
    "TEST_DATABASE_ADMIN_URL",
}
SKIP_DIRS = {".git", "node_modules", "dist", "bin", ".cache", ".venv"}


def read_secrets():
    secrets = set()
    with open(os.path.join(ROOT, ".env"), "r", encoding="utf-8") as source:
        for line in source:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key not in SECRET_KEYS:
                continue
            value = value.strip().strip("\"'")
            if len(value) >= 16 and not value.startswith("REPLACE_WITH_"):
                secrets.add(value.encode("utf-8"))
    return secrets


def repository_files():
    for current, directories, files in os.walk(ROOT):
        directories[:] = [name for name in directories if name not in SKIP_DIRS]
        for name in files:
            if name in {".env"} or name.startswith(".env.") and name != ".env.example":
                continue
            path = os.path.join(current, name)
            if os.path.islink(path) or not os.path.isfile(path):
                continue
            yield path


def main():
    if not os.path.isfile(os.path.join(ROOT, ".env")):
        print("Secret scan requires the local .env file.", file=sys.stderr)
        return 2

    secrets = read_secrets()
    for path in repository_files():
        try:
            with open(path, "rb") as source:
                content = source.read()
        except OSError:
            print("Secret scan could not read a repository file.", file=sys.stderr)
            return 2
        if any(secret in content for secret in secrets):
            print("Secret scan found a local secret value in: " + os.path.relpath(path, ROOT), file=sys.stderr)
            return 1

    container = subprocess.run(
        ["docker", "compose", "ps", "-q", "api"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if container.returncode != 0 or not container.stdout.strip():
        print("Secret scan could not locate the verification API container.", file=sys.stderr)
        return 2
    image_files = subprocess.run(
        ["docker", "compose", "exec", "-T", "api", "tar", "-C", "/app", "-cf", "-", "."],
        cwd=ROOT,
        capture_output=True,
        check=False,
    )
    if image_files.returncode != 0:
        print("Secret scan could not inspect the runtime image filesystem.", file=sys.stderr)
        return 2
    try:
        with tarfile.open(fileobj=BytesIO(image_files.stdout), mode="r:") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                source = archive.extractfile(member)
                if source is not None:
                    content = source.read()
                    if any(secret in content for secret in secrets):
                        print("Secret scan found a local secret value in the runtime image.", file=sys.stderr)
                        return 1
    except (tarfile.TarError, OSError):
        print("Secret scan could not read the runtime image filesystem archive.", file=sys.stderr)
        return 2

    logs = subprocess.run(
        ["docker", "compose", "logs", "--no-color"],
        cwd=ROOT,
        text=False,
        capture_output=True,
        check=False,
    )
    if logs.returncode != 0:
        print("Secret scan could not inspect Compose logs.", file=sys.stderr)
        return 2
    if any(secret in logs.stdout or secret in logs.stderr for secret in secrets):
        print("Secret scan found a local secret value in Compose logs.", file=sys.stderr)
        return 1

    print("Secret scan passed: local .env values are absent from repository files, runtime image files, and Compose logs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
