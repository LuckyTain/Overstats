from __future__ import annotations

import asyncio
import base64
from collections import OrderedDict
from dataclasses import dataclass, field
import json
import re
import threading
import time
from typing import Any, Dict, List, Optional

import httpx

try:
    from overstats.config import config as app_config
    from overstats.src.client.apiclient import DashenAPIClient
    from overstats.src.modules.bnet_search import BnetSearchModule, BnetSearchResult, bnet_search_module
    from overstats.src.modules.errors import ModuleError
except ModuleNotFoundError:
    from config import config as app_config
    from src.client.apiclient import DashenAPIClient
    from src.modules.bnet_search import BnetSearchModule, BnetSearchResult, bnet_search_module
    from src.modules.errors import ModuleError
from ..analysis_common import build_async_client as build_analysis_async_client
from ..analysis_common import get_analysis_proxy

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

_CACHE_LOCK = threading.RLock()
_PLAYER_TOKEN_CACHE: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_PLAYER_DETAIL_CACHE: "OrderedDict[tuple[str, str], dict[str, Any]]" = OrderedDict()
_PLAYER_CARD_CACHE: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_REPLY_CONTEXT_CACHE: "OrderedDict[str, dict[str, Any]]" = OrderedDict()


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
        image = render_match_list(matches, full_id=full_id) if render else None
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
        image = (
            render_match_detail(
                detail.payload,
                source_match=detail.source_match,
                query_full_id=query_full_id,
                query_bnet_id=query_bnet_id,
            )
            if render
            else None
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
        image = (
            render_match_detail(
                detail.payload,
                source_match=detail.source_match,
                query_full_id=resolved_bnet.full_id if resolved_bnet else query.bnet_id,
                query_bnet_id=resolved_bnet.bnet_id if resolved_bnet else "",
            )
            if render
            else None
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

        query_full_id = (
            (resolved_bnet.full_id if resolved_bnet else "")
            or (resolved_query.bnet_id if resolved_query else "")
            or (query.bnet_id if query else "")
            or f"token:{customer_token[:8]}"
        )
        query_bnet_id = (resolved_bnet.bnet_id if resolved_bnet else "") or (str(query.bnet_id or "") if query else "")

        main_image = render_match_detail(
            detail.payload,
            source_match=detail.source_match or source_match,
            query_full_id=query_full_id,
            query_bnet_id=query_bnet_id,
        )
        main_image = decorate_rendered_image_header(
            main_image,
            query_full_id,
            bnet_id=query_bnet_id,
            subtitle="角斗对局主战绩" if detail.match_kind == "fight" else "大神对局主战绩",
        )

        detail_root = _extract_match_detail_data(detail.payload)
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
            waterfall = decorate_rendered_image_header(waterfall, query_full_id, bnet_id=query_bnet_id, subtitle="全员详细数据")
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
            detail_image = decorate_rendered_image_header(detail_image, query_full_id, bnet_id=query_bnet_id, subtitle="英雄详细数据")
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
                return DashenMatchDetail(match_id=match_id, match_kind="normal", payload=payload, source_match={})
        except Exception as exc:
            last_error = exc
        try:
            payload = await self.requests.api_client.fight_query_match_info(customer_token, match_id)
            root = _extract_match_detail_data(payload)
            if root.get("roundCountList") or root.get("totalCount") or root.get("teammateList") or root.get("enemyList"):
                return DashenMatchDetail(match_id=match_id, match_kind="fight", payload=payload, source_match={})
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
            rank_info = player.get("rankInfo") or {}
            try:
                if int(rank_info.get("rankScore") or 0) > 0:
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
                    }
                )

        async def fetch_target(target: Dict[str, Any]) -> Dict[str, Any]:
            full_name = str(target.get("name") or "")
            if full_name.lower() == target_id.lower():
                token = await self._resolve_player_customer_token(full_name)
                try:
                    card_payload = await self._fetch_cached_player_card(token) if token else {}
                except Exception:
                    card_payload = {}
                card_data = card_payload.get("data") if isinstance(card_payload, dict) and isinstance(card_payload.get("data"), dict) else {}
                return {
                    "name": full_name,
                    "heroList": detail_root.get("heroList") or [],
                    "bnet_id": target.get("bnet_id") or query_bnet_id,
                    "rankInfo": target.get("rankInfo") or {},
                    "team_type": target.get("team_type"),
                    "icon": str(card_data.get("icon") or "").strip(),
                    "success": True,
                }
            token = await self._resolve_player_customer_token(full_name)
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
            return {
                "name": full_name,
                "heroList": root.get("heroList") or [],
                "bnet_id": target.get("bnet_id"),
                "rankInfo": target.get("rankInfo") or {},
                "team_type": target.get("team_type"),
                "icon": str(card_data.get("icon") or "").strip(),
                "success": bool(root.get("heroList")),
            }

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
            valid.insert(
                0,
                {
                    "name": target_id,
                    "heroList": detail_root.get("heroList") or [],
                    "bnet_id": query_bnet_id,
                    "rankInfo": focus_player.get("rankInfo") or {},
                    "team_type": team_type,
                    "icon": "",
                    "success": True,
                },
            )
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

    async def _build_ai_analysis(
        self,
        *,
        match_data: Dict[str, Any],
        all_player_details: List[Dict[str, Any]],
        target_id: str,
    ) -> Dict[str, Any]:
        summary_text = generate_match_summary_text(match_data, target_id)
        detailed_text = generate_detailed_stats_text(all_player_details, target_id)
        score_bundle = calculate_match_scores(match_data)
        # Prompt data must contain JSON primitives only. PIL Image instances
        # are useful to the legacy image renderer but cannot be serialized.
        carry_index_data = build_carry_index_data(match_data, include_image_icons=False)
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
            "{target_id}": target_id,
            "{match_summary}": summary_text,
            "{player_details}": detailed_text,
            "{carry_index_data}": json.dumps(carry_index_data, ensure_ascii=False),
            "{attribute_scores}": json.dumps(score_bundle, ensure_ascii=False),
            "{hero_knowledge}": hero_knowledge_json,
        }
        for placeholder, value in replacements.items():
            template = template.replace(placeholder, value)
        response_schema = {
            "players": [{
                "player_id": "exact BattleTag from input",
                "rating": "S/A/B/C/D",
                "headline": "20-40 Chinese characters",
                "analysis": "50-120 Chinese characters",
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
""".rstrip()
        prompt = f"""
{persona_prompt}
{template}

【服务端计算的全场表现评分 carry_index_data】
{json.dumps(carry_index_data, ensure_ascii=False)}

【查询方队伍属性评分 attribute_scores】
{json.dumps(score_bundle, ensure_ascii=False)}

【比赛面板数据】
{summary_text}
--------------------------------------------------
【全员英雄详细数据】
{detailed_text}
{knowledge_block}
--------------------------------------------------
【硬性输出要求】
只输出合法 JSON，禁止 Markdown、解释、前后缀。players 必须覆盖比赛面板全部玩家且保持面板顺序；player_id 必须精确使用输入中的完整 BattleTag；rating 仅能是 S/A/B/C/D。
Do not cite, infer, or construct historical averages, medians, percentiles, global benchmarks, or rank benchmarks. No historical reference sample is supplied to the model. Evaluate players only from the explicitly supplied match data, carry_index_data, and hero knowledge.
如果一个玩家表现较差，对该玩家提供建议。建议必须指出本场比赛中明显的优势或劣势，且不得将知识库回顾方向作为观察到的事件呈现。建议可以是更换其他英雄以反制对方。
{json.dumps(response_schema, ensure_ascii=False, indent=2)}
""".strip()

        base_url = str(getattr(app_config, "ANALYSIS_BASE_URL", "") or "").strip()
        api_key = _sanitize_api_key(getattr(app_config, "ANALYSIS_API_KEY", ""))

        if not base_url or not api_key:
            return {"fallback_text": "AI锐评未配置 ANALYSIS_BASE_URL / ANALYSIS_API_KEY。"}

        model = _analysis_model_for_base_url(base_url)
        try:
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
        """Keep model prose, but anchor roster and scores to real match data."""
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
                "headline": str(source.get("headline") or source.get("general_summary") or "未返回有效的一句话点评。"),
                "analysis": str(source.get("analysis") or source.get("evaluation") or "未返回该玩家的详细分析。"),
                "carry_score": int(carry.get("score") or 0),
                "advice": str(source.get("advice") or source.get("suggestion") or source.get("recommendation") or "\u6682\u65e0\u57fa\u4e8e\u672c\u5c40\u6570\u636e\u7684\u660e\u786e\u6539\u8fdb\u5efa\u8bae\u3002"),
                "carry_rank": 0,
                "hero_guid": str(carry.get("hero_guid") or ""),
                "hero_icon": str(carry.get("hero_icon") or ""),
                "icon": str(detail.get("icon") or ""),
            })
        players.sort(key=lambda item: int(item["carry_score"]), reverse=True)
        for rank, player in enumerate(players, start=1):
            player["carry_rank"] = rank

        raw_match = raw.get("match") if isinstance(raw.get("match"), dict) else raw

        def card(value: Any, fallback_title: str) -> Dict[str, str]:
            source = value if isinstance(value, dict) else {}
            return {
                "title": str(source.get("title") or fallback_title),
                "player_id": str(source.get("player_id") or ""),
                "reason": str(source.get("reason") or source.get("analysis") or value or "未返回有效结论。"),
            }

        return {
            "schema_version": "v2",
            "target_player_id": target_id,
            "generated_at": time.strftime("%Y-%m-%d %H:%M", time.localtime()),
            "carry_index_data": carry_index_data,
            "players": players,
            "match": {
                "win_condition": card(raw_match.get("win_condition") or raw.get("key_to_win_loss"), "唯一胜负手"),
                "mvp": card(raw_match.get("mvp"), "MVP"),
                "liability": card(raw_match.get("liability") or raw_match.get("scapegoat"), "背锅位"),
                "summary": str(raw_match.get("summary") or raw.get("summary") or "未返回有效的一句话总结。"),
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

    def _match_result_text(self, match_data: Dict[str, Any]) -> str:
        if match_data.get("matchRet") == 1:
            return "胜利"
        if match_data.get("matchRet") == 0:
            return "平局"
        return "失败"


dashen_match_module = DashenMatchModule()
