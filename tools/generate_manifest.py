"""
generate_manifest.py

Walks the demo folder and computes SHA256 checksums and sizes for files, then
updates deliverables_manifest.json in the demo root.

Run: python tools/generate_manifest.py
"""
import hashlib
import json
import os
from pathlib import Path

root = Path(__file__).resolve().parents[1]
manifest_path = root / 'deliverables_manifest.json'

files = []
for p in sorted(root.rglob('*')):
    if p.is_file():
        rel = str(p.relative_to(root)).replace('\\','/')
        # skip the manifest itself to avoid recursion
        if rel == 'deliverables_manifest.json':
            continue
        h = hashlib.sha256()
        with open(p, 'rb') as fh:
            while True:
                chunk = fh.read(8192)
                if not chunk:
                    break
                h.update(chunk)
        files.append({
            'path': rel,
            'sha256': h.hexdigest(),
            'size_bytes': p.stat().st_size
        })

out = {
    'manifest_generated_at': __import__('datetime').datetime.utcnow().isoformat() + 'Z',
    'files': files
}

with open(manifest_path, 'w', encoding='utf-8') as f:
    json.dump(out, f, indent=2)

print(f'Wrote {manifest_path} with {len(files)} files')
