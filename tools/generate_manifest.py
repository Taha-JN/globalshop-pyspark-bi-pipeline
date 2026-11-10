"""
generate_manifest.py

Walks the project directory and computes SHA256 checksums and sizes for files,
then updates deliverables_manifest.json in the project root.
Excludes version control, virtual environments, cache, and generated artifacts.

Usage:
    python tools/generate_manifest.py
"""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = PROJECT_ROOT / "deliverables_manifest.json"

EXCLUDED_DIRS = {".git", ".venv", "venv", "env", "__pycache__", ".pytest_cache", "output"}
EXCLUDED_FILES = {"deliverables_manifest.json"}


def should_include(path: Path) -> bool:
    for part in path.parts:
        if part in EXCLUDED_DIRS:
            return False
    if path.name in EXCLUDED_FILES:
        return False
    if path.name.endswith(".pyc") or path.name.endswith(".log"):
        return False
    return True


def generate_manifest():
    files = []
    for p in sorted(PROJECT_ROOT.rglob("*")):
        if p.is_file() and should_include(p.relative_to(PROJECT_ROOT)):
            rel = str(p.relative_to(PROJECT_ROOT)).replace("\\", "/")
            h = hashlib.sha256()
            with open(p, "rb") as fh:
                while True:
                    chunk = fh.read(8192)
                    if not chunk:
                        break
                    h.update(chunk)
            files.append({
                "path": rel,
                "sha256": h.hexdigest(),
                "size_bytes": p.stat().st_size
            })

    out = {
        "manifest_generated_at": datetime.now(timezone.utc).isoformat(),
        "files_count": len(files),
        "files": files
    }

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {MANIFEST_PATH} with {len(files)} files.")


if __name__ == "__main__":
    generate_manifest()
