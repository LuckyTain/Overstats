from __future__ import annotations

import asyncio
import base64
import copy
from collections import OrderedDict
from dataclasses import dataclass, field
import ipaddress
import json
import os
import re
import secrets
import socket
import threading
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

import httpx

try:
    from overstats.config import config as app_config
    from overstats.src.client.apiclient import DashenAPIClient
    from overstats.src.constants.ranks import get_rank_score
    from overstats.src.modules.bnet_search import BnetSearchModule, BnetSearchResult, bnet_search_module
    from overstats.src.modules.errors import ModuleError
    from overstats.src.modules.risk_status import RiskStatus, parse_risk_status
except ModuleNotFoundError:
    from config import config as app_config
    from src.client.apiclient import DashenAPIClient
    from src.constants.ranks import get_rank_score
    from src.modules.bnet_search import BnetSearchModule, BnetSearchResult, bnet_search_module
    from src.modules.errors import ModuleError
    from src.modules.risk_status import RiskStatus, parse_risk_status
from ..analysis_common import build_async_client as build_analysis_async_client
from ..analysis_common import get_analysis_proxy
from ...db import analysis_cache

from .enhanced_render import (
    build_carry_index_data,
    build_target_hero_icons,
    calculate_match_scores,
    decorate_rendered_image_header,
    generate_detailed_stats_text,
    generate_match_summary_text,
    map_icon_image_for_match,
    map_name_for_match,
    render_all_players_waterfall,
    render_analysis_report,
    render_player_hero_detail,
)
from .hero_knowledge import build_match_hero_knowledge
from .render import (
    RenderedImage,
    _extract_match_detail_data,
    _load_ow_config,
    _sort_players,
    render_match_detail,
    render_match_list,
)
from .requests import DashenMatchDetail, DashenMatchQuery, DashenMatchRequests


PLAYER_TOKEN_CACHE_TTL = 600
PLAYER_TOKEN_CACHE_MAX = 512
PLAYER_DETAIL_CACHE_TTL = 1800
PLAYER_DETAIL_CACHE_MAX = 512
PLAYER_CARD_CACHE_TTL = 1800
PLAYER_CARD_CACHE_MAX = 512
REPLY_CONTEXT_CACHE_TTL = 1800
REPLY_CONTEXT_CACHE_MAX = 256

ANALYSIS_RATE_LIMIT_REQUESTS = int(getattr(app_config, "ANALYSIS_RATE_LIMIT_REQUESTS", 20) or 20)
ANALYSIS_RATE_LIMIT_WINDOW_SECONDS = int(getattr(app_config, "ANALYSIS_RATE_LIMIT_WINDOW_SECONDS", 60) or 60)
ANALYSIS_CACHE_TTL_SECONDS = int(getattr(app_config, "ANALYSIS_CACHE_TTL_SECONDS", 86400) or 86400)
ANALYSIS_CACHE_VERSION = str(getattr(app_config, "ANALYSIS_CACHE_VERSION", "v1") or "v1")
ANALYSIS_CACHE_NAMESPACE = "server-default"
BYOK_CONTEXT_TTL_SECONDS = 900
ANALYSIS_JOB_MAX_CONCURRENT = max(1, int(os.getenv("OVERSTATS_ANALYSIS_JOB_MAX_CONCURRENT", str(getattr(app_config, "ANALYSIS_JOB_MAX_CONCURRENT", 2))) or 2))
ANALYSIS_JOB_MAX_QUEUED = max(1, int(os.getenv("OVERSTATS_ANALYSIS_JOB_MAX_QUEUED", str(getattr(app_config, "ANALYSIS_JOB_MAX_QUEUED", 20))) or 20))
ANALYSIS_JOB_TTL_SECONDS = max(300, int(os.getenv("OVERSTATS_ANALYSIS_JOB_TTL_SECONDS", str(getattr(app_config, "ANALYSIS_JOB_TTL_SECONDS", 3600))) or 3600))

_CACHE_LOCK = threading.RLock()
_PLAYER_TOKEN_CACHE: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_PLAYER_DETAIL_CACHE: "OrderedDict[tuple[str, str], dict[str, Any]]" = OrderedDict()
_PLAYER_CARD_CACHE: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_REPLY_CONTEXT_CACHE: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_ANALYSIS_RATE_LOCK = threading.RLock()
_ANALYSIS_RATE_TIMES: List[float] = []
_ANALYSIS_LOCK_GUARD = threading.RLock()
_ANALYSIS_LOCKS: Dict[str, asyncio.Lock] = {}
_BYOK_CONTEXTS: "OrderedDict[str, dict[str, Any]]" = OrderedDict()

_ANALYSIS_JOB_LOCK = asyncio.Lock()
_ANALYSIS_JOB_SEMAPHORE_INSTANCE: Optional[asyncio.Semaphore] = None
_ANALYSIS_JOBS: "OrderedDict[str, dict[str, Any]]" = OrderedDict()


def _hero_lookup_keys(value: Any) -> set[str]:
    """Return comparable decimal and raw forms for hero ids from upstream/config."""
    text = str(value or "").strip()
    if not text:
        return set()
    keys = {text}
    try:
        if text.lower().startswith("0x"):
            keys.add(str(int(text, 16)))
        elif text.isdigit():
            keys.add(str(int(text)))
    except (TypeError, ValueError):
        pass
    return keys


def _build_hero_metadata_lookup(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for item in list(config.get("heroList") or []):
        if not isinstance(item, dict):
            continue
        for key in ("heroGuid", "heroId", "guid", "id"):
            for lookup_key in _hero_lookup_keys(item.get(key)):
                lookup.setdefault(lookup_key, item)
    return lookup


def _enrich_hero_item(hero: Dict[str, Any], lookup: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    enriched = dict(hero)
    hero_keys = set()
    for key in ("heroGuid", "heroId", "hero_guid", "hero_id", "guid", "id"):
        hero_keys.update(_hero_lookup_keys(hero.get(key)))
    metadata = next((lookup[key] for key in hero_keys if key in lookup), {})

    name = str(
        hero.get("heroName")
        or hero.get("hero_name")
        or hero.get("name")
        or metadata.get("name")
        or ""
    ).strip()
    icon = str(
        hero.get("heroIcon")
        or hero.get("hero_icon")
        or hero.get("smallIconUrl")
        or hero.get("ddHeroIcon")
        or hero.get("icon")
        or metadata.get("smallIconUrl")
        or metadata.get("ddHeroIcon")
        or metadata.get("icon")
        or ""
    ).strip()
    role = str(
        hero.get("heroRole")
        or hero.get("hero_role")
        or hero.get("roleType")
        or hero.get("role")
        or metadata.get("roleType")
        or metadata.get("role")
        or ""
    ).strip()

    if name:
        enriched["heroName"] = name
    if icon:
        enriched["heroIcon"] = icon
    if role:
        enriched["heroRole"] = role
    return enriched


def _enrich_match_detail_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Attach query_tool hero metadata without exposing the full config."""
    config = _load_ow_config()
    lookup = _build_hero_metadata_lookup(config)
    if not lookup:
        return payload

    enriched = copy.deepcopy(payload)

    def visit(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
            return
        if not isinstance(value, dict):
            return
        for key, child in list(value.items()):
            if key in {"heroList", "hero_list"} and isinstance(child, list):
                value[key] = [
                    _enrich_hero_item(item, lookup) if isinstance(item, dict) else item
                    for item in child
                ]
            visit(value[key])

    visit(enriched)
    return enriched
_ANALYSIS_JOB_KEYS: Dict[str, str] = {}
_ANALYSIS_JOB_TASKS: Dict[str, asyncio.Task[Any]] = {}


def _analysis_job_error(exc: Exception) -> Dict[str, Any]:
    if isinstance(exc, ModuleError):
        return {"code": exc.error, "message": exc.message, "hint": exc.hint, "details": exc.details}
    return {"code": "analysis_failed", "message": "AI analysis failed. Please retry later."}


def _analysis_job_public(job: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        "ok": True,
        "status": str(job.get("status") or "failed"),
        "job_id": str(job.get("job_id") or ""),
        "match_id": str(job.get("match_id") or ""),
        "poll_after_ms": 2000,
    }
    if job.get("status") == "completed":
        result["result"] = job.get("result")
    elif job.get("status") == "failed":
        error = job.get("error") if isinstance(job.get("error"), dict) else {}
        result.update({
            "error": str(error.get("code") or "analysis_failed"),
            "message": str(error.get("message") or "AI analysis failed. Please retry later."),
            "hint": error.get("hint"),
            "details": error.get("details") if isinstance(error.get("details"), dict) else {},
        })
    return result


async def _analysis_job_semaphore() -> asyncio.Semaphore:
    global _ANALYSIS_JOB_SEMAPHORE_INSTANCE
    if _ANALYSIS_JOB_SEMAPHORE_INSTANCE is None:
        _ANALYSIS_JOB_SEMAPHORE_INSTANCE = asyncio.Semaphore(ANALYSIS_JOB_MAX_CONCURRENT)
    return _ANALYSIS_JOB_SEMAPHORE_INSTANCE


async def _run_analysis_job(job_id: str, producer: Any) -> None:
    job = _ANALYSIS_JOBS.get(job_id)
    if not job:
        return
    try:
        semaphore = await _analysis_job_semaphore()
        async with semaphore:
            job["status"] = "running"
            job["updated_at"] = time.time()
            result = await producer()
        job["status"] = "completed"
        job["result"] = result
        job["updated_at"] = time.time()
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = _analysis_job_error(exc)
        job["updated_at"] = time.time()


async def _submit_analysis_job(*, key: str, match_id: str, producer: Any) -> Dict[str, Any]:
    now = time.time()
    async with _ANALYSIS_JOB_LOCK:
        for job_id, job in list(_ANALYSIS_JOBS.items()):
            if float(job.get("expires_at", 0)) <= now:
                _ANALYSIS_JOBS.pop(job_id, None)
                if _ANALYSIS_JOB_KEYS.get(str(job.get("key") or "")) == job_id:
                    _ANALYSIS_JOB_KEYS.pop(str(job.get("key") or ""), None)
        if len(_ANALYSIS_JOBS) >= 250:
            for job_id, job in list(_ANALYSIS_JOBS.items()):
                if job.get("status") in {"completed", "failed"}:
                    _ANALYSIS_JOBS.pop(job_id, None)
                    if _ANALYSIS_JOB_KEYS.get(str(job.get("key") or "")) == job_id:
                        _ANALYSIS_JOB_KEYS.pop(str(job.get("key") or ""), None)
                    if len(_ANALYSIS_JOBS) < 250:
                        break
        existing_id = _ANALYSIS_JOB_KEYS.get(key)
        existing = _ANALYSIS_JOBS.get(existing_id or "")
        if existing and existing.get("status") in {"queued", "running", "completed"}:
            return _analysis_job_public(existing)
        active_count = sum(1 for item in _ANALYSIS_JOBS.values() if item.get("status") in {"queued", "running"})
        if active_count >= ANALYSIS_JOB_MAX_QUEUED + ANALYSIS_JOB_MAX_CONCURRENT:
            raise ModuleError(
                error="analysis_job_queue_full",
                message="AI analysis queue is full. Please retry later.",
                status_code=429,
                details={"retry_after_seconds": 10, "queue_limit": ANALYSIS_JOB_MAX_QUEUED},
            )
        job_id = secrets.token_urlsafe(24)
        job = {
            "job_id": job_id,
            "key": key,
            "match_id": match_id,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "expires_at": now + ANALYSIS_JOB_TTL_SECONDS,
        }
        _ANALYSIS_JOBS[job_id] = job
        _ANALYSIS_JOB_KEYS[key] = job_id
        task = asyncio.create_task(_run_analysis_job(job_id, producer))
        _ANALYSIS_JOB_TASKS[job_id] = task
        task.add_done_callback(lambda _, completed_id=job_id: _ANALYSIS_JOB_TASKS.pop(completed_id, None))
        return _analysis_job_public(job)


async def get_analysis_job(job_id: str) -> Dict[str, Any]:
    now = time.time()
    async with _ANALYSIS_JOB_LOCK:
        job = _ANALYSIS_JOBS.get(str(job_id or "").strip())
        if not job or float(job.get("expires_at", 0)) <= now:
            raise ModuleError(error="analysis_job_not_found", message="AI analysis job was not found or has expired.", status_code=404)
        return _analysis_job_public(job)


def _analysis_cache_meta(created_at: float, expires_at: float, *, hit: bool) -> Dict[str, Any]:
    return {
        "scope": "server",
        "hit": hit,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(created_at)),
        "expires_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_at)),
        "ttl_seconds": ANALYSIS_CACHE_TTL_SECONDS,
    }


def _acquire_analysis_rate_slot() -> None:
    now = time.monotonic()
    with _ANALYSIS_RATE_LOCK:
        cutoff = now - ANALYSIS_RATE_LIMIT_WINDOW_SECONDS
        _ANALYSIS_RATE_TIMES[:] = [stamp for stamp in _ANALYSIS_RATE_TIMES if stamp > cutoff]
        if len(_ANALYSIS_RATE_TIMES) >= ANALYSIS_RATE_LIMIT_REQUESTS:
            retry_after = max(1, int(_ANALYSIS_RATE_TIMES[0] + ANALYSIS_RATE_LIMIT_WINDOW_SECONDS - now + 0.999))
            raise ModuleError(
                error="analysis_rate_limited",
                message="AI 锐评请求过于频繁，请稍后重试。",
                status_code=429,
                details={
                    "limit": ANALYSIS_RATE_LIMIT_REQUESTS,
                    "window_seconds": ANALYSIS_RATE_LIMIT_WINDOW_SECONDS,
                    "retry_after_seconds": retry_after,
                },
            )
        _ANALYSIS_RATE_TIMES.append(now)


def _analysis_lock(match_id: str) -> asyncio.Lock:
    with _ANALYSIS_LOCK_GUARD:
        lock = _ANALYSIS_LOCKS.get(match_id)
        if lock is None:
            lock = asyncio.Lock()
            _ANALYSIS_LOCKS[match_id] = lock
        return lock


def _purge_byok_contexts() -> None:
    now = time.time()
    with _CACHE_LOCK:
        for request_id, context in list(_BYOK_CONTEXTS.items()):
            if float(context.get("expires_at", 0)) <= now:
                _BYOK_CONTEXTS.pop(request_id, None)


async def _validated_public_byok_url(endpoint: str) -> str:
    value = str(endpoint or "").strip().rstrip("/")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ModuleError(error="invalid_byok_endpoint", message="BYOK API endpoint is invalid.", status_code=400) from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ModuleError(error="invalid_byok_endpoint", message="BYOK API endpoint must be an absolute HTTP or HTTPS URL.", status_code=400)
    if parsed.username or parsed.password:
        raise ModuleError(error="invalid_byok_endpoint", message="BYOK API endpoint must not contain URL credentials.", status_code=400)
    if parsed.fragment:
        raise ModuleError(error="invalid_byok_endpoint", message="BYOK API endpoint must not contain a URL fragment.", status_code=400)
    try:
        addresses = await asyncio.to_thread(socket.getaddrinfo, parsed.hostname, port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ModuleError(error="byok_proxy_dns_failed", message="BYOK API endpoint could not be resolved.", status_code=400) from exc
    resolved = {str(item[4][0]).split("%", 1)[0] for item in addresses if item and item[4]}
    if not resolved or any(not ipaddress.ip_address(address).is_global for address in resolved):
        raise ModuleError(
            error="byok_proxy_endpoint_blocked",
            message="BYOK proxy only allows public HTTP or HTTPS endpoints.",
            status_code=403,
        )
    return value


_CARRY_CLAIM_RE = re.compile(
    r"[^，,。！？!?；;\n]*(?:carry\s*index|carry\s*指数|carry指数|全场表现评分)"
    r"[^，,。！？!?；;\n]*(?:[，,。！？!?；;]|$)",
    flags=re.IGNORECASE,
)
_MODEL_SUPERLATIVE_RE = re.compile(
    r"(?:全场|本场|双方|队内|全队|己方|敌方|本队|对方|同职责(?:内)?|同位置(?:内)?)"
    r"(?:数据)?(?:并列)?(?:最多|最少|最高|最低|第[一1](?:名)?|倒数第[一1](?:名)?|垫底)"
    r"|(?:高于|低于)(?:全场|本场|双方|队内|己方|敌方|本队|对方)?所有(?:选手|玩家|人)"
)


def _sanitize_model_analysis_text(value: Any, fallback: str = "") -> str:
    """Keep model prose, but remove claims owned by deterministic server facts."""
    text = str(value or "").strip()
    if not text:
        return fallback
    text = text.replace("焦点玩家", "")
    text = _CARRY_CLAIM_RE.sub("", text)
    text = _MODEL_SUPERLATIVE_RE.sub("", text)
    text = re.sub(r"\s{2,}", " ", text).strip(" ，,。；;\n\t")
    return text or fallback


def _build_match_stat_facts(
    match_data: Dict[str, Any],
    carry_index_data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build global, deterministic scoreboard facts for the LLM and response."""
    metric_fields = {
        "kills": ("kill",),
        "assists": ("assist",),
        "deaths": ("death",),
        "final_hits": ("finalBlows", "finalHit"),
        "objective_time": ("targetCompetingTime",),
        "hero_damage": ("heroDamage",),
        "damage_taken": ("damageTaken",),
        "healing": ("cure",),
        "healing_taken": ("healingTaken",),
        "damage_mitigated": ("resistDamage",),
    }
    carry_by_id = {
        str(item.get("player_id") or "").strip().lower(): item
        for item in carry_index_data
        if isinstance(item, dict)
    }
    players: List[Dict[str, Any]] = []
    for team, player_list in (("teammate", match_data.get("teammateList") or []), ("enemy", match_data.get("enemyList") or [])):
        for player in player_list:
            if not isinstance(player, dict):
                continue
            player_id = str(player.get("name") or "").strip()
            carry = carry_by_id.get(player_id.lower(), {})
            metrics: Dict[str, Any] = {}
            for metric, candidates in metric_fields.items():
                raw_value: Any = 0
                for field_name in candidates:
                    if player.get(field_name) is not None:
                        raw_value = player.get(field_name)
                        break
                try:
                    numeric = float(raw_value or 0)
                    metrics[metric] = int(numeric) if numeric.is_integer() else round(numeric, 2)
                except (TypeError, ValueError):
                    metrics[metric] = 0
            players.append({
                "player_id": player_id,
                "team": team,
                "role": str(carry.get("role") or ""),
                "metrics": metrics,
            })

    extrema: Dict[str, Dict[str, Any]] = {}
    for metric in metric_fields:
        values = [float(item["metrics"].get(metric, 0) or 0) for item in players]
        if not values:
            continue
        maximum = max(values)
        minimum = min(values)
        extrema[metric] = {
            "max_value": int(maximum) if maximum.is_integer() else round(maximum, 2),
            "max_player_ids": [
                item["player_id"]
                for item in players
                if float(item["metrics"].get(metric, 0) or 0) == maximum
            ],
            "min_value": int(minimum) if minimum.is_integer() else round(minimum, 2),
            "min_player_ids": [
                item["player_id"]
                for item in players
                if float(item["metrics"].get(metric, 0) or 0) == minimum
            ],
        }
    return {
        "scope": "all_players",
        "player_count": len(players),
        "players": players,
        "extrema": extrema,
    }


def _cache_get(cache: OrderedDict, key: Any) -> Any:
    with _CACHE_LOCK:
        item = cache.get(key)
        if not isinstance(item, dict):
            return None
        if time.time() >= float(item.get("expiry", 0) or 0):
            cache.pop(key, None)
            return None
        try:
            cache.move_to_end(key)
        except Exception:
            pass
        return item.get("value")


def _cache_put(cache: OrderedDict, key: Any, value: Any, *, ttl: int, max_size: int) -> None:
    with _CACHE_LOCK:
        now = time.time()
        cache[key] = {"value": value, "created_at": now, "expiry": now + max(1, int(ttl))}
        try:
            cache.move_to_end(key)
        except Exception:
            pass
        while len(cache) > max_size:
            cache.popitem(last=False)


def _sanitize_api_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text or "replace-with-your" in text.lower():
        return ""
    return text


def _analysis_model_for_base_url(base_url: str) -> str:
    normalized = str(base_url or "").strip().lower()
    if "generativelanguage.googleapis.com" in normalized or "googleapis.com" in normalized:
        return str(getattr(app_config, "ANALYSIS_GOOGLE_MODEL", "gemini-3.1-flash-lite-preview") or "gemini-3.1-flash-lite-preview")
    if "deepseek" in normalized:
        return str(getattr(app_config, "ANALYSIS_DEEPSEEK_MODEL", "deepseek-chat") or "deepseek-chat")
    return str(getattr(app_config, "ANALYSIS_OPENAI_MODEL", "gpt-4o-mini") or "gpt-4o-mini")


def _resolved_payload(resolved: Optional[BnetSearchResult]) -> Optional[Dict[str, Any]]:
    if not resolved:
        return None
    return {
        "query": resolved.query,
        "full_id": resolved.full_id,
        "bnet_id": resolved.bnet_id,
        "customer_token": resolved.customer_token,
        "has_customer_token": bool(resolved.customer_token),
    }


def _image_reply(rendered: RenderedImage) -> Dict[str, Any]:
    return {
        "type": "image",
        "media_type": rendered.media_type,
        "base64": base64.b64encode(rendered.content).decode("ascii"),
    }


def _text_reply(text: str) -> Dict[str, Any]:
    return {"type": "text", "data": str(text or "")}


def _meta_reply(meta_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": "meta", "meta_type": meta_type, "data": data}


def _extract_llm_message_content(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message")
            if isinstance(message, dict):
                return str(message.get("content") or "")
    data = payload.get("data")
    if isinstance(data, dict):
        return _extract_llm_message_content(data)
    return ""


def _clean_llm_text(text: Any) -> str:
    cleaned = str(text or "")
    cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    return cleaned


def _parse_analysis_json(text: Any) -> Optional[Dict[str, Any]]:
    cleaned = _clean_llm_text(text)
    if not cleaned:
        return None
    candidates = [cleaned]
    candidates.extend(re.findall(r"\{.*\}", cleaned, flags=re.DOTALL))
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


@dataclass(frozen=True)
class DashenMatchListOutput:
    matches: List[Dict[str, Any]]
    customer_token: str
    resolved_bnet: Optional[BnetSearchResult] = None
    image: Optional[RenderedImage] = None


@dataclass(frozen=True)
class DashenMatchDetailOutput:
    detail: DashenMatchDetail
    customer_token: str
    resolved_bnet: Optional[BnetSearchResult] = None
    image: Optional[RenderedImage] = None


@dataclass(frozen=True)
class DashenMatchRepliesOutput:
    customer_token: str
    resolved_bnet: Optional[BnetSearchResult] = None
    replies: List[Dict[str, Any]] = field(default_factory=list)
    match_id: str = ""
    match_kind: str = ""


@dataclass(frozen=True)
class DashenMatchAnalysisOutput:
    customer_token: str
    match_id: str
    match_kind: str
    analysis: Dict[str, Any] = field(default_factory=dict)
    cache: Dict[str, Any] = field(default_factory=dict)


class DashenMatchModule:
    def __init__(
        self,
        api_client: Optional[DashenAPIClient] = None,
        search_module: Optional[BnetSearchModule] = None,
    ) -> None:
        self.requests = DashenMatchRequests(api_client)
        self.search_module = search_module or bnet_search_module

    async def query_match_list(self, query: DashenMatchQuery, *, render: bool = True) -> DashenMatchListOutput:
        query, resolved_bnet = await self._resolve_query(query)
        matches = await self.requests.list_recent_matches(query)
        full_id = resolved_bnet.full_id if resolved_bnet else (query.bnet_id or f"token:{query.customer_token}")
        image = None
        if render:
            risk_status = await self._fetch_player_risk_status(query.customer_token)
            render_kwargs: Dict[str, Any] = {"full_id": full_id}
            if risk_status is not None:
                render_kwargs["risk_status"] = risk_status
            image = render_match_list(matches, **render_kwargs)
        self._store_reply_context(query, resolved_bnet, matches)
        return DashenMatchListOutput(
            matches=matches,
            customer_token=query.customer_token,
            resolved_bnet=resolved_bnet,
            image=image,
        )

    async def query_match_list_replies(self, query: DashenMatchQuery) -> DashenMatchRepliesOutput:
        result = await self.query_match_list(query, render=True)
        full_id = result.resolved_bnet.full_id if result.resolved_bnet else (query.bnet_id or f"token:{result.customer_token}")
        replies = [
            _meta_reply(
                "ds_match_list",
                {
                    "context_type": "ds_match_list",
                    "full_id": full_id,
                    "resolved": _resolved_payload(result.resolved_bnet),
                    "match_entries": result.matches,
                },
            ),
        ]
        if result.image:
            replies.append(_image_reply(result.image))
        return DashenMatchRepliesOutput(
            customer_token=result.customer_token,
            resolved_bnet=result.resolved_bnet,
            replies=replies,
        )

    async def query_match_detail(
        self,
        customer_token: str,
        match: Dict[str, Any] | str,
        *,
        query_full_id: str = "",
        query_bnet_id: str = "",
        render: bool = True,
    ) -> DashenMatchDetailOutput:
        detail = await self.requests.get_match_detail(customer_token, match)
        detail = DashenMatchDetail(
            match_id=detail.match_id,
            match_kind=detail.match_kind,
            payload=_enrich_match_detail_payload(detail.payload),
            source_match=detail.source_match,
        )
        image = None
        if render:
            enriched_detail = await self._hydrate_match_detail_risk_statuses(detail.payload)
            image = render_match_detail(
                enriched_detail,
                source_match=detail.source_match,
                query_full_id=query_full_id,
                query_bnet_id=query_bnet_id,
            )
        return DashenMatchDetailOutput(detail=detail, customer_token=customer_token, image=image)

    async def query_match_detail_by_index(
        self,
        query: DashenMatchQuery,
        index: int,
        *,
        render: bool = True,
    ) -> DashenMatchDetailOutput:
        query, resolved_bnet = await self._resolve_query(query)
        matches = await self._list_matches_for_query(query, resolved_bnet)
        if index < 0 or index >= len(matches):
            raise ModuleError(
                error="match_index_out_of_range",
                message=f"Match index out of range: {index}",
                status_code=400,
                hint=f"Use an index from 0 to {max(len(matches) - 1, 0)}.",
                details={"index": index, "match_count": len(matches)},
            )
        detail = await self.requests.get_match_detail(query.customer_token, matches[index])
        detail = DashenMatchDetail(
            match_id=detail.match_id,
            match_kind=detail.match_kind,
            payload=_enrich_match_detail_payload(detail.payload),
            source_match=detail.source_match,
        )
        image = None
        if render:
            enriched_detail = await self._hydrate_match_detail_risk_statuses(detail.payload)
            image = render_match_detail(
                enriched_detail,
                source_match=detail.source_match,
                query_full_id=resolved_bnet.full_id if resolved_bnet else query.bnet_id,
                query_bnet_id=resolved_bnet.bnet_id if resolved_bnet else "",
            )
        return DashenMatchDetailOutput(
            detail=detail,
            customer_token=query.customer_token,
            resolved_bnet=resolved_bnet,
            image=image,
        )

    async def query_match_detail_replies(
        self,
        *,
        query: Optional[DashenMatchQuery] = None,
        customer_token: str = "",
        match_id: str = "",
        index: Optional[int] = None,
        show_all_heroes: bool = False,
        analyze: bool = False,
    ) -> DashenMatchRepliesOutput:
        resolved_query: Optional[DashenMatchQuery] = None
        resolved_bnet: Optional[BnetSearchResult] = None
        source_match: Dict[str, Any] = {}

        if match_id:
            direct_customer_token = str(customer_token or (query.customer_token if query else "")).strip()
            if not direct_customer_token:
                raise ModuleError(
                    error="missing_customer_token",
                    message="customer_token is required when querying detail by match_id directly.",
                    status_code=400,
                    hint='Use {"bnet_id":"Player#12345","index":0} or provide customer_token with match_id.',
                )
            customer_token = direct_customer_token
            if query and query.bnet_id:
                try:
                    _, resolved_bnet = await self._resolve_query(query)
                except Exception:
                    resolved_bnet = None
            detail = await self._get_match_detail_direct(customer_token, match_id)
        else:
            if query is None or index is None:
                raise ModuleError(
                    error="missing_match_selector",
                    message="index or match_id is required for match detail.",
                    status_code=400,
                    hint='Example: {"bnet_id":"Player#12345","index":0}',
                )
            resolved_query, resolved_bnet = await self._resolve_query(query)
            customer_token = resolved_query.customer_token
            matches = await self._list_matches_for_query(resolved_query, resolved_bnet)
            if index < 0 or index >= len(matches):
                raise ModuleError(
                    error="match_index_out_of_range",
                    message=f"Match index out of range: {index}",
                    status_code=400,
                    hint=f"Use an index from 0 to {max(len(matches) - 1, 0)}.",
                    details={"index": index, "match_count": len(matches)},
                )
            source_match = dict(matches[index])
            detail = await self.requests.get_match_detail(customer_token, source_match)
        detail = DashenMatchDetail(
            match_id=detail.match_id,
            match_kind=detail.match_kind,
            payload=_enrich_match_detail_payload(detail.payload),
            source_match=detail.source_match,
        )

        query_full_id = (
            (resolved_bnet.full_id if resolved_bnet else "")
            or (resolved_query.bnet_id if resolved_query else "")
            or (query.bnet_id if query else "")
            or f"token:{customer_token[:8]}"
        )
        query_bnet_id = (resolved_bnet.bnet_id if resolved_bnet else "") or (str(query.bnet_id or "") if query else "")

        target_risk_status = await self._fetch_player_risk_status(customer_token)
        detail_root = await self._hydrate_match_detail_risk_statuses(
            detail.payload,
            known_risk_statuses={customer_token: target_risk_status},
        )
        main_image = render_match_detail(
            detail_root,
            source_match=detail.source_match or source_match,
            query_full_id=query_full_id,
            query_bnet_id=query_bnet_id,
        )
        main_header_kwargs: Dict[str, Any] = {
            "bnet_id": query_bnet_id,
            "subtitle": "角斗对局主战绩" if detail.match_kind == "fight" else "大神对局主战绩",
        }
        if target_risk_status is not None:
            main_header_kwargs["risk_status"] = target_risk_status
        main_image = decorate_rendered_image_header(main_image, query_full_id, **main_header_kwargs)

        ordered_player_ids = self._ordered_player_ids(detail_root)
        is_competitive_match = self._is_competitive_match(detail_root, detail.match_kind, detail.source_match or source_match)
        replies = [
            _meta_reply(
                "ds_match_detail_players",
                {
                    "context_type": "ds_match_detail_players",
                    "player_ids": ordered_player_ids if detail.match_kind != "fight" else [],
                    "competitive": bool(is_competitive_match),
                },
            ),
            _image_reply(main_image),
        ]

        if detail.match_kind == "fight":
            if show_all_heroes or analyze:
                replies.append(_text_reply("角斗对局暂不支持全员详细或 AI锐评。"))
            return DashenMatchRepliesOutput(
                customer_token=customer_token,
                resolved_bnet=resolved_bnet,
                replies=replies,
                match_id=detail.match_id,
                match_kind=detail.match_kind,
            )

        focus_player = self._find_focus_player(detail_root, query_full_id=query_full_id, query_bnet_id=query_bnet_id)
        focus_detail = {
            "heroList": detail_root.get("heroList") or (focus_player.get("heroList") if focus_player else []) or [],
            "rankInfo": (focus_player.get("rankInfo") if focus_player else {}) or {},
        }

        if show_all_heroes:
            player_details, target_id = await self._build_all_player_details(
                detail_root,
                detail.match_id,
                query_full_id=query_full_id,
                query_bnet_id=query_bnet_id,
            )
            waterfall = render_all_players_waterfall(player_details, match_game_time_sec=detail_root.get("gameTimeSec"))
            waterfall_header_kwargs: Dict[str, Any] = {"bnet_id": query_bnet_id, "subtitle": "全员详细数据"}
            if target_risk_status is not None:
                waterfall_header_kwargs["risk_status"] = target_risk_status
            waterfall = decorate_rendered_image_header(waterfall, query_full_id, **waterfall_header_kwargs)
            replies.append(_image_reply(waterfall))
            if analyze:
                analysis_result = await self._build_ai_analysis(
                    match_data=detail_root,
                    all_player_details=player_details,
                    target_id=target_id,
                )
                if analysis_result.get("json") is not None:
                    json_data = dict(analysis_result["json"])
                    json_data["generated_at"] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                    analysis_image = render_analysis_report(
                        json_data,
                        target_hero_images=build_target_hero_icons(focus_detail["heroList"], size=40),
                        map_name=map_name_for_match(detail_root),
                        map_icon_img=map_icon_image_for_match(detail_root),
                        match_result=self._match_result_text(detail_root),
                        footer_source=analysis_result.get("footer_source"),
                    )
                    replies.append(_image_reply(analysis_image))
                else:
                    replies.append(_text_reply(analysis_result.get("fallback_text") or "AI锐评暂不可用。"))
        else:
            detail_image = render_player_hero_detail(
                query_full_id,
                focus_detail,
                match_game_time_sec=detail_root.get("gameTimeSec"),
            )
            detail_header_kwargs: Dict[str, Any] = {"bnet_id": query_bnet_id, "subtitle": "英雄详细数据"}
            if target_risk_status is not None:
                detail_header_kwargs["risk_status"] = target_risk_status
            detail_image = decorate_rendered_image_header(detail_image, query_full_id, **detail_header_kwargs)
            replies.append(_image_reply(detail_image))

        return DashenMatchRepliesOutput(
            customer_token=customer_token,
            resolved_bnet=resolved_bnet,
            replies=replies,
            match_id=detail.match_id,
            match_kind=detail.match_kind,
        )

    async def query_match_detail_analysis(
        self,
        *,
        customer_token: str,
        match_id: str,
    ) -> DashenMatchAnalysisOutput:
        customer_token = str(customer_token or "").strip()
        match_id = str(match_id or "").strip()
        if not customer_token:
            raise ModuleError(error="missing_customer_token", message="customer_token is required for match analysis.", status_code=400)
        if not match_id:
            raise ModuleError(error="missing_match_selector", message="match_id is required for match analysis.", status_code=400)

        cached = analysis_cache.get(ANALYSIS_CACHE_NAMESPACE, ANALYSIS_CACHE_VERSION, match_id)
        if cached:
            return DashenMatchAnalysisOutput(
                customer_token=customer_token,
                match_id=match_id,
                match_kind=str(cached.get("match_kind") or "normal"),
                analysis=dict(cached["analysis"]),
                cache=_analysis_cache_meta(float(cached["created_at"]), float(cached["expires_at"]), hit=True),
            )

        async with _analysis_lock(match_id):
            cached = analysis_cache.get(ANALYSIS_CACHE_NAMESPACE, ANALYSIS_CACHE_VERSION, match_id)
            if cached:
                return DashenMatchAnalysisOutput(
                    customer_token=customer_token,
                    match_id=match_id,
                    match_kind=str(cached.get("match_kind") or "normal"),
                    analysis=dict(cached["analysis"]),
                    cache=_analysis_cache_meta(float(cached["created_at"]), float(cached["expires_at"]), hit=True),
                )
            result = await self._generate_match_detail_analysis(customer_token=customer_token, match_id=match_id)
            cache_times = analysis_cache.put(
                ANALYSIS_CACHE_NAMESPACE,
                ANALYSIS_CACHE_VERSION,
                match_id,
                result.match_kind,
                result.analysis,
                provider_model=str(getattr(app_config, "ANALYSIS_OPENAI_MODEL", "") or ""),
                ttl_seconds=ANALYSIS_CACHE_TTL_SECONDS,
            )
            return DashenMatchAnalysisOutput(
                customer_token=result.customer_token,
                match_id=result.match_id,
                match_kind=result.match_kind,
                analysis=result.analysis,
                cache=_analysis_cache_meta(cache_times["created_at"], cache_times["expires_at"], hit=False),
            )

    async def create_match_detail_analysis_job(self, *, customer_token: str, match_id: str) -> Dict[str, Any]:
        customer_token = str(customer_token or "").strip()
        match_id = str(match_id or "").strip()
        if not customer_token:
            raise ModuleError(error="missing_customer_token", message="customer_token is required for match analysis.", status_code=400)
        if not match_id:
            raise ModuleError(error="missing_match_selector", message="match_id is required for match analysis.", status_code=400)

        async def produce() -> Dict[str, Any]:
            result = await self.query_match_detail_analysis(customer_token=customer_token, match_id=match_id)
            return {
                "ok": True,
                "customer_token": result.customer_token,
                "match_id": result.match_id,
                "match_kind": result.match_kind,
                "analysis": result.analysis,
                "cache": result.cache,
            }

        return await _submit_analysis_job(key=f"server:{match_id}", match_id=match_id, producer=produce)

    async def create_byok_proxy_analysis_job(
        self,
        *,
        customer_token: str,
        match_id: str,
        endpoint: str,
        model: str,
        api_key: str,
    ) -> Dict[str, Any]:
        customer_token = str(customer_token or "").strip()
        match_id = str(match_id or "").strip()
        endpoint = str(endpoint or "").strip()
        model = str(model or "").strip()
        api_key = _sanitize_api_key(api_key)
        if not customer_token:
            raise ModuleError(error="missing_customer_token", message="customer_token is required for match analysis.", status_code=400)
        if not match_id:
            raise ModuleError(error="missing_match_selector", message="match_id is required for match analysis.", status_code=400)
        if not model:
            raise ModuleError(error="missing_byok_model", message="BYOK model is required.", status_code=400)
        if not api_key:
            raise ModuleError(error="missing_byok_api_key", message="BYOK API key is required.", status_code=400)
        endpoint = await _validated_public_byok_url(endpoint)

        import hashlib
        fingerprint = hashlib.sha256(f"{endpoint}\n{model}\n{api_key}".encode("utf-8")).hexdigest()

        async def produce() -> Dict[str, Any]:
            return await self.proxy_match_detail_analysis(
                customer_token=customer_token,
                match_id=match_id,
                endpoint=endpoint,
                model=model,
                api_key=api_key,
            )

        return await _submit_analysis_job(key=f"byok:{match_id}:{fingerprint}", match_id=match_id, producer=produce)

    async def get_analysis_job(self, job_id: str) -> Dict[str, Any]:
        return await get_analysis_job(job_id)

    async def _generate_match_detail_analysis(
        self,
        *,
        customer_token: str,
        match_id: str,
    ) -> DashenMatchAnalysisOutput:
        """Build full-roster AI analysis without rendering any image replies."""
        customer_token = str(customer_token or "").strip()
        match_id = str(match_id or "").strip()
        if not customer_token:
            raise ModuleError(
                error="missing_customer_token",
                message="customer_token is required for match analysis.",
                status_code=400,
            )
        if not match_id:
            raise ModuleError(
                error="missing_match_selector",
                message="match_id is required for match analysis.",
                status_code=400,
            )

        detail = await self._get_match_detail_direct(customer_token, match_id)
        if detail.match_kind == "fight":
            raise ModuleError(
                error="analysis_not_supported",
                message="Structured AI analysis is not available for fight matches.",
                status_code=400,
            )

        match_data = _extract_match_detail_data(detail.payload)
        try:
            card_payload = await self._fetch_cached_player_card(customer_token)
        except Exception:
            card_payload = {}
        card_data = card_payload.get("data") if isinstance(card_payload, dict) and isinstance(card_payload.get("data"), dict) else {}
        primary_player = next(
            (
                dict(player)
                for player in list(match_data.get("teammateList") or []) + list(match_data.get("enemyList") or [])
                if isinstance(player, dict) and str(player.get("name") or "").strip().lower() == str(card_data.get("name") or "").strip().lower()
            ),
            {},
        )
        if not primary_player:
            primary_player = next(
                (
                    dict(player)
                    for player in list(match_data.get("teammateList") or []) + list(match_data.get("enemyList") or [])
                    if isinstance(player, dict) and (player.get("heroList") or match_data.get("heroList"))
                ),
                {},
            )
        query_full_id = str(primary_player.get("name") or card_data.get("name") or f"token:{customer_token[:8]}").strip()
        player_details, target_id = await self._build_all_player_details(
            match_data,
            detail.match_id,
            query_full_id=query_full_id,
            query_bnet_id=str(primary_player.get("bnetId") or "").strip(),
        )
        built = await self._build_ai_analysis(
            match_data=match_data,
            all_player_details=player_details,
            target_id=target_id,
        )
        raw_analysis = built.get("json")
        if not isinstance(raw_analysis, dict):
            raise ModuleError(
                error="analysis_failed",
                message=str(built.get("fallback_text") or "AI analysis could not be generated."),
                status_code=502,
            )
        return DashenMatchAnalysisOutput(
            customer_token=customer_token,
            match_id=detail.match_id,
            match_kind=detail.match_kind,
            analysis=self._normalize_match_analysis(
                raw_analysis,
                match_data=match_data,
                all_player_details=player_details,
                target_id=target_id,
            ),
        )

    async def prepare_match_detail_analysis(self, *, customer_token: str, match_id: str) -> Dict[str, Any]:
        customer_token = str(customer_token or "").strip()
        match_id = str(match_id or "").strip()
        if not customer_token:
            raise ModuleError(error="missing_customer_token", message="customer_token is required for match analysis.", status_code=400)
        if not match_id:
            raise ModuleError(error="missing_match_selector", message="match_id is required for match analysis.", status_code=400)
        detail = await self._get_match_detail_direct(customer_token, match_id)
        if detail.match_kind == "fight":
            raise ModuleError(error="analysis_not_supported", message="Structured AI analysis is not available for fight matches.", status_code=400)
        match_data = _extract_match_detail_data(detail.payload)
        try:
            card_payload = await self._fetch_cached_player_card(customer_token)
        except Exception:
            card_payload = {}
        card_data = card_payload.get("data") if isinstance(card_payload, dict) and isinstance(card_payload.get("data"), dict) else {}
        players = list(match_data.get("teammateList") or []) + list(match_data.get("enemyList") or [])
        primary_player = next((dict(player) for player in players if isinstance(player, dict) and str(player.get("name") or "").strip().lower() == str(card_data.get("name") or "").strip().lower()), {})
        if not primary_player:
            primary_player = next((dict(player) for player in players if isinstance(player, dict) and (player.get("heroList") or match_data.get("heroList"))), {})
        query_full_id = str(primary_player.get("name") or card_data.get("name") or f"token:{customer_token[:8]}").strip()
        player_details, target_id = await self._build_all_player_details(
            match_data,
            detail.match_id,
            query_full_id=query_full_id,
            query_bnet_id=str(primary_player.get("bnetId") or "").strip(),
        )
        built = await self._build_ai_analysis(match_data=match_data, all_player_details=player_details, target_id=target_id, request_only=True)
        request_id = secrets.token_urlsafe(32)
        expires_at = time.time() + BYOK_CONTEXT_TTL_SECONDS
        with _CACHE_LOCK:
            _purge_byok_contexts()
            _BYOK_CONTEXTS[request_id] = {
                "customer_token": customer_token,
                "match_id": detail.match_id,
                "match_kind": detail.match_kind,
                "match_data": match_data,
                "player_details": player_details,
                "target_id": target_id,
                "expires_at": expires_at,
            }
        return {
            "ok": True,
            "request_id": request_id,
            "match_id": detail.match_id,
            "expires_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_at)),
            "llm_request": {
                "temperature": 0.2,
                "messages": [{"role": "user", "content": str(built.get("prompt") or "")}],
            },
        }

    async def finalize_match_detail_analysis(self, *, request_id: str, content: str) -> Dict[str, Any]:
        request_id = str(request_id or "").strip()
        with _CACHE_LOCK:
            _purge_byok_contexts()
            context = _BYOK_CONTEXTS.get(request_id)
        if not context:
            raise ModuleError(error="analysis_request_expired", message="The BYOK analysis request has expired.", status_code=410)
        parsed = _parse_analysis_json(str(content or ""))
        if not isinstance(parsed, dict):
            raise ModuleError(error="invalid_analysis_json", message="The model response did not contain valid analysis JSON.", status_code=422)
        analysis = self._normalize_match_analysis(
            parsed,
            match_data=context["match_data"],
            all_player_details=context["player_details"],
            target_id=str(context["target_id"]),
        )
        with _CACHE_LOCK:
            _BYOK_CONTEXTS.pop(request_id, None)
        return {
            "ok": True,
            "match_id": context["match_id"],
            "match_kind": context["match_kind"],
            "analysis": analysis,
            "cache": {"scope": "browser", "hit": False, "ttl_seconds": 86400},
        }

    async def proxy_match_detail_analysis(
        self,
        *,
        customer_token: str,
        match_id: str,
        endpoint: str,
        model: str,
        api_key: str,
    ) -> Dict[str, Any]:
        endpoint = await _validated_public_byok_url(endpoint)
        model = str(model or "").strip()
        api_key = _sanitize_api_key(api_key)
        if not model:
            raise ModuleError(error="missing_byok_model", message="BYOK model is required.", status_code=400)
        if not api_key:
            raise ModuleError(error="missing_byok_api_key", message="BYOK API key is required.", status_code=400)

        prepared = await self.prepare_match_detail_analysis(customer_token=customer_token, match_id=match_id)
        request_id = str(prepared.get("request_id") or "")
        llm_request = prepared.get("llm_request") if isinstance(prepared.get("llm_request"), dict) else {}
        messages = llm_request.get("messages") if isinstance(llm_request.get("messages"), list) else []
        try:
            raw = await self._call_byok_compatible(
                self._chat_completion_url(endpoint),
                api_key,
                {"model": model, "temperature": 0.2, "messages": messages},
            )
            content = _extract_llm_message_content(raw)
            if not content:
                raise ModuleError(error="invalid_analysis_json", message="The BYOK model returned no analysis content.", status_code=422)
            return await self.finalize_match_detail_analysis(request_id=request_id, content=content)
        except ModuleError:
            raise
        except httpx.TimeoutException as exc:
            raise ModuleError(error="byok_proxy_timeout", message="BYOK provider request timed out.", status_code=504) from exc
        except httpx.HTTPStatusError as exc:
            provider_status = int(exc.response.status_code)
            if provider_status in {401, 403, 429}:
                error = {401: "byok_provider_unauthorized", 403: "byok_provider_forbidden", 429: "byok_provider_rate_limited"}[provider_status]
                message = {401: "BYOK provider rejected the API key.", 403: "BYOK provider denied this request.", 429: "BYOK provider rate limit was reached."}[provider_status]
                raise ModuleError(error=error, message=message, status_code=provider_status, details={"provider_status": provider_status}) from exc
            raise ModuleError(error="byok_provider_error", message="BYOK provider returned an error.", status_code=502, details={"provider_status": provider_status}) from exc
        except (httpx.RequestError, ValueError) as exc:
            raise ModuleError(error="byok_proxy_network_error", message="BYOK provider could not be reached or returned invalid JSON.", status_code=502) from exc
        finally:
            if request_id:
                with _CACHE_LOCK:
                    _BYOK_CONTEXTS.pop(request_id, None)

    async def query_match_list_by_bnet_id(
        self,
        bnet_id: str,
        *,
        render: bool = True,
        **query_options: Any,
    ) -> DashenMatchListOutput:
        return await self.query_match_list(DashenMatchQuery(bnet_id=bnet_id, **query_options), render=render)

    async def query_match_detail_by_bnet_id(
        self,
        bnet_id: str,
        index: int,
        *,
        render: bool = True,
        **query_options: Any,
    ) -> DashenMatchDetailOutput:
        return await self.query_match_detail_by_index(
            DashenMatchQuery(bnet_id=bnet_id, **query_options),
            index,
            render=render,
        )

    async def _resolve_query(self, query: DashenMatchQuery) -> tuple[DashenMatchQuery, Optional[BnetSearchResult]]:
        if query.customer_token:
            return query, None
        if not query.bnet_id:
            raise ModuleError(
                error="missing_target",
                message="Missing query target: bnet_id or customer_token is required.",
                status_code=400,
                hint='Example: {"bnet_id":"Player#12345","limit":20}',
            )
        search_output = await self.search_module.search(query.bnet_id, render=False)
        customer_token = search_output.result.customer_token
        if not customer_token:
            payload = search_output.result.payload
            data = payload.get("data") if isinstance(payload, dict) else None
            raise ModuleError(
                error="bnet_not_found",
                message=f"Could not resolve customerToken from bnet_id: {query.bnet_id}",
                status_code=404,
                hint=(
                    "Check exact letter case and the number after '#'. "
                    "Dashen search is often case-sensitive. "
                    "If you already have customer_token, query with customer_token directly."
                ),
                details={
                    "query": search_output.result.query,
                    "upstream_code": payload.get("code") if isinstance(payload, dict) else None,
                    "upstream_msg": payload.get("msg") if isinstance(payload, dict) else None,
                    "has_data": isinstance(data, dict),
                    "has_customer_token": bool(customer_token),
                    "resolved_name": search_output.result.full_id,
                    "resolved_bnet_id": search_output.result.bnet_id,
                },
            )
        resolved_query = DashenMatchQuery(
            customer_token=customer_token,
            bnet_id=search_output.result.full_id,
            seasons=query.seasons,
            include_previous_season=query.include_previous_season,
            include_fight=query.include_fight,
            target_count=query.target_count,
            filters=query.filters,
        )
        return resolved_query, search_output.result

    def _reply_context_cache_key(self, query: DashenMatchQuery, resolved_bnet: Optional[BnetSearchResult]) -> str:
        return json.dumps(
            {
                "customer_token": query.customer_token,
                "full_id": resolved_bnet.full_id if resolved_bnet else query.bnet_id,
                "target_count": int(query.target_count),
                "include_fight": bool(query.include_fight),
                "include_previous_season": bool(query.include_previous_season),
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    def _store_reply_context(
        self,
        query: DashenMatchQuery,
        resolved_bnet: Optional[BnetSearchResult],
        matches: List[Dict[str, Any]],
    ) -> None:
        _cache_put(
            _REPLY_CONTEXT_CACHE,
            self._reply_context_cache_key(query, resolved_bnet),
            {"matches": matches, "resolved_bnet": resolved_bnet},
            ttl=REPLY_CONTEXT_CACHE_TTL,
            max_size=REPLY_CONTEXT_CACHE_MAX,
        )

    async def _list_matches_for_query(self, query: DashenMatchQuery, resolved_bnet: Optional[BnetSearchResult]) -> List[Dict[str, Any]]:
        cached = _cache_get(_REPLY_CONTEXT_CACHE, self._reply_context_cache_key(query, resolved_bnet))
        if isinstance(cached, dict) and isinstance(cached.get("matches"), list):
            return list(cached["matches"])
        matches = await self.requests.list_recent_matches(query)
        self._store_reply_context(query, resolved_bnet, matches)
        return matches

    async def _get_match_detail_direct(self, customer_token: str, match_id: str) -> DashenMatchDetail:
        last_error: Optional[Exception] = None
        try:
            payload = await self.requests.api_client.query_match_info(customer_token, match_id)
            root = _extract_match_detail_data(payload)
            if root.get("teammateList") or root.get("enemyList") or root.get("heroList"):
                return DashenMatchDetail(
                    match_id=match_id,
                    match_kind="normal",
                    payload=_enrich_match_detail_payload(payload),
                    source_match={},
                )
        except Exception as exc:
            last_error = exc
        try:
            payload = await self.requests.api_client.fight_query_match_info(customer_token, match_id)
            root = _extract_match_detail_data(payload)
            if root.get("roundCountList") or root.get("totalCount") or root.get("teammateList") or root.get("enemyList"):
                return DashenMatchDetail(
                    match_id=match_id,
                    match_kind="fight",
                    payload=_enrich_match_detail_payload(payload),
                    source_match={},
                )
        except Exception as exc:
            last_error = exc
        if last_error is not None:
            raise last_error
        return DashenMatchDetail(match_id=match_id, match_kind="normal", payload={}, source_match={})

    def _ordered_player_ids(self, detail_root: Dict[str, Any]) -> List[str]:
        config = _load_ow_config()
        ordered: List[str] = []
        for team_key in ("teammateList", "enemyList"):
            for player in _sort_players(detail_root.get(team_key, []), config):
                name = str(player.get("name") or "").strip()
                if name:
                    ordered.append(name)
        return ordered

    def _is_competitive_match(self, detail_root: Dict[str, Any], match_kind: str, source_match: Optional[Dict[str, Any]]) -> bool:
        if match_kind == "fight":
            return "sportfight" in str((source_match or {}).get("gameMode") or "").lower()
        for player in list(detail_root.get("teammateList") or []) + list(detail_root.get("enemyList") or []):
            rank_info = player.get("rankInfo") or player.get("rank_info") or {}
            try:
                if int(get_rank_score(rank_info) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                continue
        return "sport" in str((source_match or {}).get("gameMode") or detail_root.get("gameMode") or "").lower()

    def _find_focus_player(self, detail_root: Dict[str, Any], *, query_full_id: str, query_bnet_id: str) -> Dict[str, Any]:
        normalized_full = str(query_full_id or "").strip().lower()
        normalized_tag = normalized_full.split("#", 1)[0]
        normalized_bnet_id = str(query_bnet_id or "").strip()
        for player in list(detail_root.get("teammateList") or []) + list(detail_root.get("enemyList") or []):
            player_name = str(player.get("name") or "").strip().lower()
            player_tag = player_name.split("#", 1)[0]
            player_bnet_id = str(player.get("bnetId") or "").strip()
            if normalized_bnet_id and player_bnet_id and normalized_bnet_id == player_bnet_id:
                return dict(player)
            if normalized_full and player_name == normalized_full:
                return dict(player)
            if normalized_tag and player_tag == normalized_tag:
                return dict(player)
        return {}

    async def _build_all_player_details(
        self,
        detail_root: Dict[str, Any],
        match_id: str,
        *,
        query_full_id: str,
        query_bnet_id: str,
    ) -> tuple[List[Dict[str, Any]], str]:
        focus_player = self._find_focus_player(detail_root, query_full_id=query_full_id, query_bnet_id=query_bnet_id)
        target_id = str(focus_player.get("name") or query_full_id or "").strip() or query_full_id
        all_targets: List[Dict[str, Any]] = []
        for team_key, team_type in (("teammateList", "teammate"), ("enemyList", "enemy")):
            for player in detail_root.get(team_key, []) or []:
                name = str(player.get("name") or "").strip()
                if not name or "#" not in name:
                    continue
                all_targets.append(
                    {
                        "name": name,
                        "team_type": team_type,
                        "rankInfo": player.get("rankInfo") or {},
                        "bnet_id": str(player.get("bnetId") or ""),
                        "customer_token": str(player.get("customerToken") or player.get("customer_token") or "").strip(),
                        "risk_status": player.get("riskStatus") or player.get("risk_status"),
                    }
                )

        async def fetch_target(target: Dict[str, Any]) -> Dict[str, Any]:
            full_name = str(target.get("name") or "")
            token = str(target.get("customer_token") or "").strip()
            if not token:
                token = await self._resolve_player_customer_token(full_name)
            if full_name.lower() == target_id.lower():
                try:
                    card_payload = await self._fetch_cached_player_card(token) if token else {}
                except Exception:
                    card_payload = {}
                card_data = card_payload.get("data") if isinstance(card_payload, dict) and isinstance(card_payload.get("data"), dict) else {}
                result = {
                    "name": full_name,
                    "heroList": detail_root.get("heroList") or [],
                    "bnet_id": target.get("bnet_id") or query_bnet_id,
                    "rankInfo": target.get("rankInfo") or {},
                    "team_type": target.get("team_type"),
                    "icon": str(card_data.get("icon") or "").strip(),
                    "success": True,
                }
                risk_status = parse_risk_status(card_payload) or parse_risk_status(target.get("risk_status"))
                if risk_status is not None:
                    result["riskStatus"] = risk_status.to_dict()
                return result
            if not token:
                return {"name": full_name, "team_type": target.get("team_type"), "success": False}
            detail_payload, card_payload = await asyncio.gather(
                self._fetch_cached_player_match_detail(token, match_id),
                self._fetch_cached_player_card(token),
                return_exceptions=True,
            )
            payload = detail_payload if isinstance(detail_payload, dict) else {}
            root = _extract_match_detail_data(payload)
            card_data = card_payload.get("data") if isinstance(card_payload, dict) and isinstance(card_payload.get("data"), dict) else {}
            result = {
                "name": full_name,
                "heroList": root.get("heroList") or [],
                "bnet_id": target.get("bnet_id"),
                "rankInfo": target.get("rankInfo") or {},
                "team_type": target.get("team_type"),
                "icon": str(card_data.get("icon") or "").strip(),
                "success": bool(root.get("heroList")),
            }
            risk_status = parse_risk_status(card_payload) or parse_risk_status(target.get("risk_status"))
            if risk_status is not None:
                result["riskStatus"] = risk_status.to_dict()
            return result

        # Dashen may throttle a burst of player-card / match-detail lookups.
        # This method is used by the all-player analysis path, so fetching a
        # ten-player roster concurrently can turn one user action into dozens
        # of upstream requests at once. Keep the roster walk serial; the
        # per-player cache still avoids repeated network work on later runs.
        results: List[Dict[str, Any] | BaseException] = []
        for target in all_targets:
            try:
                results.append(await fetch_target(target))
            except Exception as exc:
                results.append(exc)
        valid: List[Dict[str, Any]] = []
        for result in results:
            if isinstance(result, Exception) or not isinstance(result, dict):
                continue
            if result.get("success") and result.get("heroList"):
                valid.append(result)

        if not any(str(item.get("name") or "").lower() == target_id.lower() for item in valid) and detail_root.get("heroList"):
            team_type = "teammate"
            focus_name = str(focus_player.get("name") or "").strip()
            for enemy in detail_root.get("enemyList", []) or []:
                if str(enemy.get("name") or "").strip() == focus_name:
                    team_type = "enemy"
                    break
            fallback = {
                "name": target_id,
                "heroList": detail_root.get("heroList") or [],
                "bnet_id": query_bnet_id,
                "rankInfo": focus_player.get("rankInfo") or {},
                "team_type": team_type,
                "icon": "",
                "success": True,
            }
            fallback_risk_status = parse_risk_status(focus_player.get("riskStatus") or focus_player.get("risk_status"))
            if fallback_risk_status is not None:
                fallback["riskStatus"] = fallback_risk_status.to_dict()
            valid.insert(0, fallback)
        ordered_names = {name.lower(): idx for idx, name in enumerate(self._ordered_player_ids(detail_root))}
        valid.sort(
            key=lambda item: (
                ordered_names.get(str(item.get("name") or "").lower(), 10_000),
                0 if item.get("team_type") == "teammate" else 1,
                str(item.get("name") or "").lower(),
            )
        )
        return valid, target_id

    async def _resolve_player_customer_token(self, full_name: str) -> str:
        cache_key = str(full_name or "").strip().lower()
        cached = _cache_get(_PLAYER_TOKEN_CACHE, cache_key)
        if isinstance(cached, str):
            return cached
        try:
            search_output = await self.search_module.search(full_name, render=False)
        except Exception:
            return ""
        customer_token = search_output.result.customer_token
        if customer_token:
            _cache_put(_PLAYER_TOKEN_CACHE, cache_key, customer_token, ttl=PLAYER_TOKEN_CACHE_TTL, max_size=PLAYER_TOKEN_CACHE_MAX)
        return customer_token

    async def _fetch_cached_player_match_detail(self, customer_token: str, match_id: str) -> Dict[str, Any]:
        cache_key = (str(customer_token or ""), str(match_id or ""))
        cached = _cache_get(_PLAYER_DETAIL_CACHE, cache_key)
        if isinstance(cached, dict):
            return cached
        payload = await self.requests.api_client.query_match_info(customer_token, match_id)
        payload = _enrich_match_detail_payload(payload)
        _cache_put(_PLAYER_DETAIL_CACHE, cache_key, payload, ttl=PLAYER_DETAIL_CACHE_TTL, max_size=PLAYER_DETAIL_CACHE_MAX)
        return payload

    async def _fetch_cached_player_card(self, customer_token: str) -> Dict[str, Any]:
        cache_key = str(customer_token or "").strip()
        if not cache_key:
            return {}
        cached = _cache_get(_PLAYER_CARD_CACHE, cache_key)
        if isinstance(cached, dict):
            return cached
        payload = await self.requests.api_client.query_card(cache_key)
        card_data = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else {}
        icon_url = str(card_data.get("icon") or "").strip()
        if icon_url:
            try:
                await self.requests.api_client.get_icon(icon_url)
            except Exception:
                pass
        _cache_put(_PLAYER_CARD_CACHE, cache_key, payload, ttl=PLAYER_CARD_CACHE_TTL, max_size=PLAYER_CARD_CACHE_MAX)
        return payload

    async def _fetch_player_risk_status(self, customer_token: str) -> Optional[RiskStatus]:
        try:
            return parse_risk_status(await self._fetch_cached_player_card(customer_token))
        except Exception:
            return None

    async def _hydrate_match_detail_risk_statuses(
        self,
        payload: Dict[str, Any],
        *,
        known_risk_statuses: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Copy match detail and attach queryCard restrictions to rendered players."""

        detail_root = copy.deepcopy(_extract_match_detail_data(payload))
        players: List[Dict[str, Any]] = []

        def collect_from(container: Any) -> None:
            if not isinstance(container, dict):
                return
            for team_key in ("teammateList", "enemyList"):
                for player in container.get(team_key, []) or []:
                    if isinstance(player, dict):
                        players.append(player)

        collect_from(detail_root)
        collect_from(detail_root.get("totalCount"))
        for round_data in detail_root.get("roundCountList", []) or []:
            collect_from(round_data)

        players_by_token: Dict[str, List[Dict[str, Any]]] = {}
        for player in players:
            customer_token = str(player.get("customerToken") or player.get("customer_token") or "").strip()
            if customer_token:
                players_by_token.setdefault(customer_token, []).append(player)

        normalized_known: Dict[str, Optional[RiskStatus]] = {}
        for customer_token, value in (known_risk_statuses or {}).items():
            token = str(customer_token or "").strip()
            if token:
                normalized_known[token] = parse_risk_status(value)

        missing_tokens = [token for token in players_by_token if token not in normalized_known]
        fetched = await asyncio.gather(
            *(self._fetch_player_risk_status(token) for token in missing_tokens),
            return_exceptions=True,
        )
        for token, result in zip(missing_tokens, fetched):
            normalized_known[token] = result if isinstance(result, RiskStatus) else None

        for token, token_players in players_by_token.items():
            risk_status = normalized_known.get(token)
            if risk_status is None:
                continue
            normalized = risk_status.to_dict()
            for player in token_players:
                player["riskStatus"] = dict(normalized)

        return detail_root

    async def _build_ai_analysis(
        self,
        *,
        match_data: Dict[str, Any],
        all_player_details: List[Dict[str, Any]],
        target_id: str,
        request_only: bool = False,
    ) -> Dict[str, Any]:
        summary_text = generate_match_summary_text(match_data, target_id)
        detailed_text = generate_detailed_stats_text(all_player_details, target_id)
        score_bundle = calculate_match_scores(match_data)
        # Prompt data must contain JSON primitives only. PIL Image instances
        # are useful to the legacy image renderer but cannot be serialized.
        carry_index_data = build_carry_index_data(match_data, include_image_icons=False)
        match_stat_facts = _build_match_stat_facts(match_data, carry_index_data)
        try:
            hero_knowledge = build_match_hero_knowledge(match_data, all_player_details, _load_ow_config())
        except Exception as exc:
            print(f"[overstats] failed to build match hero knowledge: {exc}")
            hero_knowledge = {"available": False, "heroes": [], "players": []}
        hero_knowledge_json = json.dumps(hero_knowledge, ensure_ascii=False)
        persona_prompt = str(getattr(app_config, "ANALYSIS_PERSONA_PROMPT", "") or "").strip()
        template = str(getattr(app_config, "ANALYSIS_MATCH_PROMPT", "") or "").strip()
        if not template:
            template = "Analyze the supplied Overwatch match data objectively."
        replacements = {
            "{target_id}": "",
            "{match_summary}": summary_text,
            "{player_details}": detailed_text,
            "{carry_index_data}": json.dumps(carry_index_data, ensure_ascii=False),
            "{match_stat_facts}": json.dumps(match_stat_facts, ensure_ascii=False),
            "{attribute_scores}": json.dumps(score_bundle, ensure_ascii=False),
            "{hero_knowledge}": hero_knowledge_json,
        }
        for placeholder, value in replacements.items():
            template = template.replace(placeholder, value)
        response_schema = {
            "players": [{
                "player_id": "exact BattleTag from input",
                "rating": "S/A/B/C/D",
                "headline": "20-40 Chinese characters; no focal-player wording, Carry Index claims, or self-calculated superlatives",
                "analysis": "50-120 Chinese characters; no Carry Index claims or self-calculated superlatives",
                "advice": "40-100 Chinese characters; actionable and based on visible match evidence",
            }],
            "match": {
                "win_condition": {"title": "唯一胜负手", "analysis": "one decisive factor and evidence"},
                "mvp": {"player_id": "exact BattleTag", "reason": "evidence"},
                "liability": {"player_id": "exact BattleTag or empty string", "reason": "evidence or no clear liability"},
                "summary": "20-40 Chinese characters",
            },
        }
        knowledge_block = ""
        if hero_knowledge.get("available"):
            knowledge_block = f"""

[Local hero review knowledge for heroes actually used in this match]
{hero_knowledge_json}

The fixed carry_index_data score is authoritative. Hero knowledge may explain visible metrics and guide advice, but must not alter that score. Treat suggestion rules as review directions, never as proof that an event occurred. Do not invent ability hits, positioning, timing, or decisions absent from the supplied data.
Do not cite, infer, or construct historical averages, medians, percentiles, global benchmarks, or rank benchmarks. No historical reference sample is supplied to the model. Evaluate players only from the explicitly supplied match data, carry_index_data, and hero knowledge.
如果一个玩家表现较差，对该玩家提供建议。建议必须指出本场比赛中明显的优势或劣势，且不得将知识库回顾方向作为观察到的事件呈现。建议可以是更换其他英雄以反制对方。
""".rstrip()
        fact_policy_block = f"""

[Server-verified global match facts]
{json.dumps(match_stat_facts, ensure_ascii=False)}

This is a global review of all players. There is no focal player, favored player, or player that should receive extra attention. The request's customer token and target_id are data-fetching context only and must not influence ratings, MVP, liability, wording, or analysis priority.
carry_index_data is already calculated and sorted by the server. Its score, overall_rank, team_rank, role_rank, and team_role_rank fields are authoritative. Never calculate, sort, compare, reinterpret, or contradict Carry Index. Do not mention Carry Index scores or comparisons in prose; the client renders them from structured fields.
match_stat_facts is the only authority for claims such as match-high, match-low, team-high, team-low, most, least, highest, lowest, first, or last. Never infer an extreme from the prose tables. If multiple player_ids share an extremum, it is a tie and must not be described as a unique extreme.
MVP and liability must be chosen from the whole roster. Liability may use an empty player_id when no single player is decisively responsible. Never default liability to the player whose token was used to fetch the match.
""".rstrip()
        prompt = f"""
{persona_prompt}
{template}

【服务端计算的全场表现评分 carry_index_data】
{json.dumps(carry_index_data, ensure_ascii=False)}

[Server-verified global scoreboard facts: match_stat_facts]
{json.dumps(match_stat_facts, ensure_ascii=False)}

【查询方队伍属性评分 attribute_scores】
{json.dumps(score_bundle, ensure_ascii=False)}

【比赛面板数据】
{summary_text}
--------------------------------------------------
【全员英雄详细数据】
{detailed_text}
{fact_policy_block}
{knowledge_block}
--------------------------------------------------
【硬性输出要求】
只输出合法 JSON，禁止 Markdown、解释、前后缀。players 必须覆盖比赛面板全部玩家且保持面板顺序；player_id 必须精确使用输入中的完整 BattleTag；rating 仅能是 S/A/B/C/D。
{json.dumps(response_schema, ensure_ascii=False, indent=2)}
""".strip()

        base_url = str(getattr(app_config, "ANALYSIS_BASE_URL", "") or "").strip()
        model = _analysis_model_for_base_url(base_url)
        if request_only:
            return {
                "prompt": prompt,
                "model": model,
                "temperature": 0.2,
            }
        api_key = _sanitize_api_key(getattr(app_config, "ANALYSIS_API_KEY", ""))

        if not base_url or not api_key:
            return {"fallback_text": "AI锐评未配置 ANALYSIS_BASE_URL / ANALYSIS_API_KEY。"}

        try:
            _acquire_analysis_rate_slot()
            raw = await self._call_openai_compatible(
                self._chat_completion_url(base_url),
                api_key,
                {
                    "model": model,
                    "temperature": 0.2,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            parsed = _parse_analysis_json(_extract_llm_message_content(raw))
            if parsed:
                return {
                    "json": parsed,
                    "footer_source": "AI锐评",
                }
            return {"fallback_text": f"AI锐评生成失败。错误：invalid json ({model})"}
        except ModuleError:
            # Preserve typed service errors such as the 20 RPM limit so the
            # HTTP layer can return the documented status and Retry-After.
            raise
        except Exception as exc:
            return {"fallback_text": f"AI锐评生成失败。错误：{type(exc).__name__}: {exc}"}

    def _normalize_match_analysis(
        self,
        raw: Dict[str, Any],
        *,
        match_data: Dict[str, Any],
        all_player_details: List[Dict[str, Any]],
        target_id: str,
    ) -> Dict[str, Any]:
        """Keep model prose, but anchor the global roster and facts to server data."""
        roster: List[Dict[str, str]] = []
        for team_name, players in (("teammate", match_data.get("teammateList") or []), ("enemy", match_data.get("enemyList") or [])):
            for player in players:
                if not isinstance(player, dict):
                    continue
                player_id = str(player.get("name") or "").strip()
                if player_id:
                    roster.append({"player_id": player_id, "team": team_name})
        raw_players = raw.get("players") if isinstance(raw.get("players"), list) else []
        raw_by_id = {
            str(item.get("player_id") or item.get("id") or "").strip().lower(): item
            for item in raw_players
            if isinstance(item, dict)
        }
        detail_by_id = {
            str(item.get("name") or "").strip().lower(): item
            for item in all_player_details
            if isinstance(item, dict)
        }
        # The structured endpoint returns this list directly as JSON, so do
        # not attach the PIL icons used by the legacy report renderer.
        carry_index_data = build_carry_index_data(match_data, include_image_icons=False)
        match_stat_facts = _build_match_stat_facts(match_data, carry_index_data)
        carry_by_id = {
            str(item.get("player_id") or "").strip().lower(): item
            for item in carry_index_data
            if isinstance(item, dict)
        }
        players: List[Dict[str, Any]] = []
        for roster_player in roster:
            player_id = roster_player["player_id"]
            source = raw_by_id.get(player_id.lower(), {})
            carry = carry_by_id.get(player_id.lower(), {})
            detail = detail_by_id.get(player_id.lower(), {})
            rating = str(source.get("rating") or source.get("score") or "B").upper()
            if rating not in {"S", "A", "B", "C", "D"}:
                rating = "B"
            players.append({
                "player_id": player_id,
                "display_name": player_id.split("#", 1)[0],
                "team": roster_player["team"],
                "rating": rating,
                "headline": _sanitize_model_analysis_text(source.get("headline") or source.get("general_summary"), "未返回有效的一句话点评。"),
                "analysis": _sanitize_model_analysis_text(source.get("analysis") or source.get("evaluation"), "未返回该玩家的详细分析。"),
                "carry_score": int(carry.get("score") or 0),
                "advice": _sanitize_model_analysis_text(source.get("advice") or source.get("suggestion") or source.get("recommendation"), "\u6682\u65e0\u57fa\u4e8e\u672c\u5c40\u6570\u636e\u7684\u660e\u786e\u6539\u8fdb\u5efa\u8bae\u3002"),
                "carry_rank": int(carry.get("overall_rank") or 0),
                "carry_count": int(carry.get("overall_count") or 0),
                "team_rank": int(carry.get("team_rank") or 0),
                "team_count": int(carry.get("team_count") or 0),
                "role": str(carry.get("role") or ""),
                "role_rank": carry.get("role_rank"),
                "role_count": int(carry.get("role_count") or 0),
                "team_role_rank": carry.get("team_role_rank"),
                "team_role_count": int(carry.get("team_role_count") or 0),
                "hero_guid": str(carry.get("hero_guid") or ""),
                "hero_icon": str(carry.get("hero_icon") or ""),
                "icon": str(detail.get("icon") or ""),
            })
        players.sort(key=lambda item: int(item["carry_score"]), reverse=True)
        raw_match = raw.get("match") if isinstance(raw.get("match"), dict) else raw

        def card(value: Any, fallback_title: str) -> Dict[str, str]:
            source = value if isinstance(value, dict) else {}
            return {
                "title": str(source.get("title") or fallback_title),
                "player_id": str(source.get("player_id") or ""),
                "reason": _sanitize_model_analysis_text(source.get("reason") or source.get("analysis") or value, "未返回有效结论。"),
            }

        return {
            "schema_version": "v2",
            "analysis_scope": "all_players",
            "generated_at": time.strftime("%Y-%m-%d %H:%M", time.localtime()),
            "carry_index_data": carry_index_data,
            "match_stat_facts": match_stat_facts,
            "players": players,
            "match": {
                "win_condition": card(raw_match.get("win_condition") or raw.get("key_to_win_loss"), "唯一胜负手"),
                "mvp": card(raw_match.get("mvp"), "MVP"),
                "liability": card(raw_match.get("liability") or raw_match.get("scapegoat"), "背锅位"),
                "summary": _sanitize_model_analysis_text(raw_match.get("summary") or raw.get("summary"), "未返回有效的一句话总结。"),
            },
        }

    def _chat_completion_url(self, base_url: str) -> str:
        base = str(base_url or "").rstrip("/")
        if base.endswith("/chat/completions"):
            return base
        if base.endswith("/v1") or base.endswith("/openai"):
            return f"{base}/chat/completions"
        return f"{base}/chat/completions"

    async def _call_openai_compatible(self, url: str, api_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        proxy_url = get_analysis_proxy(url)
        async with build_analysis_async_client(timeout=300, proxy_url=proxy_url) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()

    async def _call_byok_compatible(self, url: str, api_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        proxy_url = get_analysis_proxy(url)
        async with build_analysis_async_client(timeout=300, proxy_url=proxy_url, follow_redirects=False) as client:
            response = await client.post(url, json=payload, headers=headers)
            if response.is_redirect:
                raise ModuleError(error="byok_proxy_redirect_blocked", message="BYOK proxy does not follow provider redirects.", status_code=502)
            response.raise_for_status()
            return response.json()

    def _match_result_text(self, match_data: Dict[str, Any]) -> str:
        if match_data.get("matchRet") == 1:
            return "胜利"
        if match_data.get("matchRet") == 0:
            return "平局"
        return "失败"


dashen_match_module = DashenMatchModule()
