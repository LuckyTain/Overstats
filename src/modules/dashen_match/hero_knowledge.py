from __future__ import annotations

from copy import deepcopy
import html
import json
from pathlib import Path
import re
import threading
from typing import Any, Dict, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_KNOWLEDGE_DIR = PROJECT_ROOT / "res" / "hero_knowledge"
GAME_TIME_GUID = "603482350067646497"
MIN_SECONDARY_USAGE_RATIO = 0.15
MAX_HEROES_PER_PLAYER = 2
MAX_UNIQUE_HEROES = 16

_CACHE_LOCK = threading.RLock()
_CACHE_SIGNATURE: tuple[tuple[str, int, int], ...] = ()
_CACHE_ENTRIES: tuple[Dict[str, Any], ...] = ()
_SECTION_LIMITS = {
    "\u6838\u5fc3\u5b9a\u4f4d": 220,
    "\u91cd\u70b9\u8bc4\u4ef7\u7ef4\u5ea6": 520,
    "\u6570\u636e\u89e3\u91ca\u6ce8\u610f\u4e8b\u9879": 300,
    "\u5efa\u8bae\u751f\u6210\u89c4\u5219": 420,
    "\u4e0d\u53ef\u63a8\u65ad\u4e8b\u9879": 280,
    "\u6280\u80fd\u6458\u8981": 180,
    "\u5a01\u80fd": 160,
}


def _normalize_key(value: Any) -> str:
    return re.sub(r"[\W_]+", "", str(value or "").strip().casefold())


def _parse_scalar(value: str) -> Any:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, list) else text
        except Exception:
            return [item.strip().strip('"').strip("'") for item in text[1:-1].split(",") if item.strip()]
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    return text


def _parse_front_matter(text: str) -> tuple[Dict[str, Any], str]:
    normalized = str(text or "").replace("\r\n", "\n")
    if not normalized.startswith("---\n"):
        return {}, normalized
    end = normalized.find("\n---\n", 4)
    if end < 0:
        return {}, normalized
    metadata: Dict[str, Any] = {}
    active_list = ""
    for raw_line in normalized[4:end].splitlines():
        line = raw_line.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        list_match = re.match(r"^\s+-\s+(.+?)\s*$", line)
        if active_list and list_match:
            metadata.setdefault(active_list, []).append(_parse_scalar(list_match.group(1)))
            continue
        field_match = re.match(r"^([A-Za-z_][A-Za-z0-9_-]*):\s*(.*?)\s*$", line)
        if not field_match:
            active_list = ""
            continue
        key, raw_value = field_match.groups()
        metadata[key] = _parse_scalar(raw_value) if raw_value else []
        active_list = "" if raw_value else key
    return metadata, normalized[end + 5 :]


def _compact_markdown(value: str, limit: int) -> str:
    lines = []
    for raw_line in str(value or "").splitlines():
        line = re.sub(r"\s+", " ", raw_line.strip())
        if line:
            lines.append(re.sub(r"^[-*+]\s+", "- ", line))
    compact = "\n".join(lines)
    return compact if len(compact) <= limit else compact[: limit - 1].rstrip() + "?"


def _extract_sections(body: str) -> Dict[str, str]:
    sections: Dict[str, list[str]] = {}
    current = ""
    for raw_line in str(body or "").splitlines():
        heading = re.match(r"^#\s+(.+?)\s*$", raw_line.strip())
        if heading:
            current = heading.group(1).strip()
            sections.setdefault(current, [])
        elif current:
            sections[current].append(raw_line)
    return {
        name: _compact_markdown("\n".join(sections[name]), limit)
        for name, limit in _SECTION_LIMITS.items()
        if sections.get(name)
    }


def _knowledge_signature(directory: Path) -> tuple[tuple[str, int, int], ...]:
    ignored = {"readme.md", "schema.md"}
    signature = []
    for path in sorted(directory.glob("*.md"), key=lambda item: item.name.casefold()):
        if path.name.casefold() in ignored:
            continue
        stat = path.stat()
        signature.append((path.name, stat.st_mtime_ns, stat.st_size))
    return tuple(signature)


def load_hero_knowledge(directory: Path | str = DEFAULT_KNOWLEDGE_DIR) -> list[Dict[str, Any]]:
    global _CACHE_SIGNATURE, _CACHE_ENTRIES
    root = Path(directory)
    if not root.is_dir():
        return []
    try:
        signature = _knowledge_signature(root)
    except OSError as exc:
        print(f"[overstats] failed to inspect hero knowledge: {exc}")
        return []
    with _CACHE_LOCK:
        if signature == _CACHE_SIGNATURE and _CACHE_ENTRIES:
            return deepcopy(list(_CACHE_ENTRIES))

    entries: list[Dict[str, Any]] = []
    for filename, _, _ in signature:
        path = root / filename
        try:
            metadata, body = _parse_front_matter(path.read_text(encoding="utf-8"))
            if int(metadata.get("schema_version") or 0) != 1:
                raise ValueError("unsupported schema_version")
            required = ("hero_guid", "hero_name", "hero_name_en", "role")
            if any(not str(metadata.get(key) or "").strip() for key in required):
                raise ValueError("missing required front matter")
            aliases = metadata.get("aliases") if isinstance(metadata.get("aliases"), list) else []
            archetypes = metadata.get("archetypes") if isinstance(metadata.get("archetypes"), list) else []
            keys = [metadata.get("hero_guid"), metadata.get("hero_name"), metadata.get("hero_name_en"), path.stem, *aliases]
            entries.append({
                "hero_key": str(metadata["hero_guid"]).strip(),
                "hero_name": str(metadata["hero_name"]).strip(),
                "hero_name_en": str(metadata["hero_name_en"]).strip(),
                "role": str(metadata["role"]).strip(),
                "sub_role": str(metadata.get("sub_role") or "").strip(),
                "archetypes": [str(item).strip() for item in archetypes if str(item).strip()],
                "knowledge_version": str(metadata.get("knowledge_version") or "").strip(),
                "updated_at": str(metadata.get("updated_at") or "").strip(),
                "sections": _extract_sections(body),
                "_match_keys": sorted({_normalize_key(item) for item in keys if _normalize_key(item)}),
            })
        except Exception as exc:
            print(f"[overstats] skipped invalid hero knowledge {path.name}: {exc}")

    with _CACHE_LOCK:
        _CACHE_SIGNATURE = signature
        _CACHE_ENTRIES = tuple(deepcopy(entries))
    return deepcopy(entries)


def _guid_candidates(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    candidates = [text]
    try:
        number = int(text, 16) if text.lower().startswith("0x") else int(text)
    except ValueError:
        return candidates
    candidates.extend((str(number), f"0x{number:016X}", f"0x0{number:015X}"))
    return list(dict.fromkeys(candidates))


def _find_hero_meta(config: Mapping[str, Any], raw_guid: Any) -> Dict[str, Any]:
    candidates = set(_guid_candidates(raw_guid))
    normalized = _normalize_key(raw_guid)
    for item in config.get("heroList", []) or []:
        if not isinstance(item, dict):
            continue
        values = [item.get(key) for key in ("heroGuid", "heroId", "guid", "id")]
        if any(candidate in candidates for value in values for candidate in _guid_candidates(value)):
            return item
        if normalized and normalized in {_normalize_key(item.get("name")), _normalize_key(item.get("nameEn"))}:
            return item
    return {}


def _find_knowledge(entries: Sequence[Mapping[str, Any]], *values: Any) -> Dict[str, Any]:
    keys = {_normalize_key(value) for value in values if _normalize_key(value)}
    if not keys:
        return {}
    for entry in entries:
        if keys.intersection(entry.get("_match_keys") or []):
            return dict(entry)
    return {}


def _safe_float(value: Any) -> float:
    try:
        return max(0.0, float(value or 0))
    except (TypeError, ValueError):
        return 0.0


def _hero_seconds(hero: Mapping[str, Any]) -> float:
    direct = _safe_float(hero.get("userTimeSec") or hero.get("useTimeSec") or hero.get("usage_seconds"))
    if direct:
        return direct
    stat_map = hero.get("statMap") if isinstance(hero.get("statMap"), dict) else {}
    return _safe_float(stat_map.get(GAME_TIME_GUID) or stat_map.get("gameTime") or stat_map.get("timePlayed"))


def _parse_listish(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return [item.strip() for item in value.split(",") if item.strip()]


def _extract_perks(player: Mapping[str, Any], hero: Mapping[str, Any] | None = None) -> list[Any]:
    for key in ("perks", "perkList", "heroPerks", "perkGuids", "perkGuidList"):
        parsed = _parse_listish((hero or {}).get(key))
        if parsed:
            return parsed
    hero_guid = (hero or {}).get("heroGuid") or (hero or {}).get("heroId") or (hero or {}).get("guid") or (hero or {}).get("id")
    player_guid = player.get("heroGuid") or player.get("heroId") or player.get("guid") or player.get("id")
    if set(_guid_candidates(hero_guid)).intersection(_guid_candidates(player_guid)):
        for key in ("perks", "perkList", "heroPerks", "perkGuids", "perkGuidList"):
            parsed = _parse_listish(player.get(key))
            if parsed:
                return parsed
    return []


def _strip_html(value: Any) -> str:
    text = re.sub(r"<br\s*/?>", ": ", str(value or ""), flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", text))).strip()


def _perk_lookup(config: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    flat_groups: list[Any] = [config.get("perkList") or []]
    hero_groups = config.get("heroPerkList") or config.get("perk") or {}
    flat_groups.extend(hero_groups.values() if isinstance(hero_groups, dict) else [hero_groups])
    for group in flat_groups:
        if not isinstance(group, list):
            continue
        for item in group:
            if not isinstance(item, dict):
                continue
            for raw in (item.get("guid"), item.get("id"), item.get("perkGuid")):
                for candidate in _guid_candidates(raw):
                    lookup[candidate] = item
    return lookup


def _resolve_perks(perks: Sequence[Any], lookup: Mapping[str, Mapping[str, Any]]) -> list[Dict[str, str]]:
    resolved = []
    seen = set()
    for perk in perks:
        values = [perk.get(key) for key in ("guid", "id", "perkGuid", "value")] if isinstance(perk, dict) else [perk]
        info: Mapping[str, Any] = {}
        raw_id = ""
        for raw in values:
            if raw is None:
                continue
            raw_id = raw_id or str(raw)
            for candidate in _guid_candidates(raw):
                if candidate in lookup:
                    info = lookup[candidate]
                    break
            if info:
                break
        name = _strip_html(info.get("name") if info else (perk.get("name") if isinstance(perk, dict) else ""))
        key = str(info.get("id") or info.get("guid") or raw_id).strip()
        if key and key not in seen:
            seen.add(key)
            resolved.append({"perk_id": key, "name": name or key})
    return resolved


def build_match_hero_knowledge(
    match_data: Mapping[str, Any],
    all_player_details: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    *,
    directory: Path | str = DEFAULT_KNOWLEDGE_DIR,
) -> Dict[str, Any]:
    entries = load_hero_knowledge(directory)
    if not entries:
        return {"available": False, "heroes": [], "players": []}

    scoreboard_by_name: Dict[str, Mapping[str, Any]] = {}
    for player in list(match_data.get("teammateList") or []) + list(match_data.get("enemyList") or []):
        if isinstance(player, dict):
            name = str(player.get("name") or "").strip().casefold()
            if name:
                scoreboard_by_name[name] = player

    perk_lookup = _perk_lookup(config)
    knowledge_by_key: Dict[str, Dict[str, Any]] = {}
    players_context = []
    for detail in all_player_details:
        if not isinstance(detail, Mapping):
            continue
        player_id = str(detail.get("name") or detail.get("player_id") or "").strip()
        scoreboard = scoreboard_by_name.get(player_id.casefold(), {})
        hero_list = [item for item in detail.get("heroList", []) or [] if isinstance(item, dict)]
        hero_list.sort(key=_hero_seconds, reverse=True)
        total_seconds = sum(_hero_seconds(hero) for hero in hero_list)
        selected = []
        for index, hero in enumerate(hero_list):
            seconds = _hero_seconds(hero)
            ratio = seconds / total_seconds if total_seconds > 0 else (1.0 if index == 0 else 0.0)
            if index > 0 and ratio < MIN_SECONDARY_USAGE_RATIO:
                continue
            raw_guid = hero.get("heroGuid") or hero.get("heroId") or hero.get("guid") or hero.get("id")
            meta = _find_hero_meta(config, raw_guid)
            knowledge = _find_knowledge(
                entries,
                raw_guid,
                hero.get("name"),
                hero.get("heroName"),
                meta.get("name"),
                meta.get("nameEn"),
            )
            if not knowledge:
                continue
            hero_key = str(knowledge["hero_key"])
            if hero_key not in knowledge_by_key and len(knowledge_by_key) < MAX_UNIQUE_HEROES:
                public_knowledge = {key: value for key, value in knowledge.items() if not key.startswith("_")}
                knowledge_by_key[hero_key] = public_knowledge
            if hero_key not in knowledge_by_key:
                continue
            perks = _extract_perks(scoreboard, hero)
            selected.append({
                "hero_key": hero_key,
                "hero_name": knowledge["hero_name"],
                "usage_seconds": round(seconds, 1),
                "usage_ratio": round(ratio, 4),
                "selected_perks": _resolve_perks(perks, perk_lookup),
            })
            if len(selected) >= MAX_HEROES_PER_PLAYER:
                break
        if selected:
            players_context.append({"player_id": player_id, "heroes": selected})

    return {
        "available": bool(knowledge_by_key),
        "policy": {
            "score_source": "carry_index_data is authoritative; hero knowledge must not change its fixed score.",
            "evidence_rule": "Knowledge explains metrics and generates review advice only. Never claim an event, skill use, positioning, or timing that the supplied match data cannot prove.",
        },
        "heroes": list(knowledge_by_key.values()),
        "players": players_context,
    }


__all__ = [
    "DEFAULT_KNOWLEDGE_DIR",
    "GAME_TIME_GUID",
    "build_match_hero_knowledge",
    "load_hero_knowledge",
]
