import hashlib
import json
import logging
import shutil
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


def _row_count(path):
    with open(path, "rb") as f:
        return sum(1 for _ in f) - 1


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def extract(source_paths, raw_folder):
    """Copy the source files into raw_folder, converting Excel to CSV, and write manifest.json.

    source_paths: {"regional": path, "categories": path, "duration": path}
    Returns {name: path of the CSV written to raw_folder}.
    """
    start = time.perf_counter()
    raw_folder = Path(raw_folder)
    raw_folder.mkdir(parents=True, exist_ok=True)

    raw_paths = {}
    manifest = []

    for name, source in source_paths.items():
        source = Path(source)

        if source.suffix == ".xlsx":
            out_path = raw_folder / source.stem
            pd.read_excel(source).to_csv(out_path, index=False)
            log.info("%s: converted Excel %s to CSV", name, source.name)
        else:
            out_path = raw_folder / source.name
            shutil.copy(source, out_path)
            log.info("%s: copied %s", name, source.name)

        entry = {
            "file_name": out_path.name,
            "size_bytes": out_path.stat().st_size,
            "sha256": _sha256(out_path),
            "row_count": _row_count(out_path),
            "extracted_at": datetime.now().isoformat(),
        }
        manifest.append(entry)
        log.info("%s: %s rows, %s bytes", name, f"{entry['row_count']:,}", f"{entry['size_bytes']:,}")
        raw_paths[name] = out_path

    manifest_path = raw_folder / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    log.info("Manifest written to %s", manifest_path)

    log.info("Extract finished in %.1f seconds", time.perf_counter() - start)
    return raw_paths
