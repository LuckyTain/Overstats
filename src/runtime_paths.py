from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_DATA_DIR = Path(os.getenv("OVERSTATS_DATA_DIR", str(PROJECT_ROOT))).expanduser()
RESOURCE_DIR = Path(os.getenv("OVERSTATS_RESOURCE_DIR", str(PROJECT_ROOT / "res"))).expanduser()
_CUSTOM_DATA_DIR = bool(os.getenv("OVERSTATS_DATA_DIR"))


def runtime_path(*parts: str) -> Path:
    if not _CUSTOM_DATA_DIR and parts:
        head, *tail = parts
        legacy_roots = {
            "db": PROJECT_ROOT / "src" / "db",
            "cache_img": PROJECT_ROOT / "res" / "cache_img",
            "query_tool_assets": PROJECT_ROOT / "res" / "query_tool_assets",
            "cache": PROJECT_ROOT / "cache",
        }
        if head in legacy_roots:
            return legacy_roots[head].joinpath(*tail)
    return RUNTIME_DATA_DIR.joinpath(*parts)


def resource_path(*parts: str) -> Path:
    return RESOURCE_DIR.joinpath(*parts)


def ensure_parent(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
