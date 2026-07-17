"""Provider-local RSS and Atom polling helpers."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit, urlunsplit
from xml.etree import ElementTree

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorBindingDraft,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorCursor,
    ConnectorCursorUpdate,
    ConnectorEvent,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorOperationContext,
    ConnectorProvenance,
    ConnectorResourceDraft,
    ConnectorResourceResult,
    ConnectorSetupResult,
    ConnectorStateUpdate,
    ConnectorSyncResult,
)
from keel_core.connector_network import (
    ConnectorHttpClient,
    ConnectorHttpPolicy,
)

FEED_POLL_CADENCE_SECONDS = 900
FEED_MAX_RESPONSE_BYTES = 1_048_576
FEED_MAX_ITEMS = 200
FEED_MAX_ELEMENTS = 5_000
FEED_MAX_DEPTH = 32
FEED_MAX_TEXT_CHARS = 32_768
FEED_MAX_TITLE_CHARS = 512
FEED_MAX_IDENTIFIER_CHARS = 2_048
FEED_MAX_SEEN_IDS = 256
_CURSOR_STREAM = "feed"
_XML_FORBIDDEN = re.compile(br"<!\s*(?:DOCTYPE|ENTITY)\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class FeedItem:
    source_id: str | None
    title: str
    url: str | None
    published: str | None
    content: str


@dataclass(frozen=True, slots=True)
class ParsedFeed:
    title: str
    items: tuple[FeedItem, ...]


FeedParser = Callable[[bytes, str], ParsedFeed]


class _HtmlTextExtractor(HTMLParser):
    def __init__(self, limit: int) -> None:
        super().__init__(convert_charrefs=True)
        self._limit = limit
        self._parts: list[str] = []
        self._size = 0
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style"}:
            self._ignored_depth += 1
        elif not self._ignored_depth and tag.lower() in {"br", "p", "div", "li", "tr"}:
            self._append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self._ignored_depth:
            self._ignored_depth -= 1
        elif not self._ignored_depth and tag.lower() in {"p", "div", "li", "tr"}:
            self._append(" ")

    def handle_data(self, data: str) -> None:
        if not self._ignored_depth:
            self._append(data)

    def _append(self, value: str) -> None:
        if self._size >= self._limit:
            return
        remaining = self._limit - self._size
        chunk = value[:remaining]
        self._parts.append(chunk)
        self._size += len(chunk)

    def text(self) -> str:
        return _normalize_whitespace("".join(self._parts))[: self._limit]


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def sanitize_feed_text(value: str, *, limit: int = FEED_MAX_TEXT_CHARS) -> str:
    extractor = _HtmlTextExtractor(limit)
    try:
        extractor.feed(value[: limit * 2])
        extractor.close()
    except ValueError:
        return _normalize_whitespace(value)[:limit]
    return extractor.text()


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _children(element: ElementTree.Element, name: str) -> list[ElementTree.Element]:
    return [child for child in element if _local_name(child.tag) == name]


def _first_child(element: ElementTree.Element, *names: str) -> ElementTree.Element | None:
    wanted = set(names)
    return next((child for child in element if _local_name(child.tag) in wanted), None)


def _element_text(element: ElementTree.Element | None, *, limit: int) -> str:
    if element is None:
        return ""
    raw = "".join(element.itertext())
    return sanitize_feed_text(raw, limit=limit)


def _parse_xml(body: bytes) -> ElementTree.Element:
    if not body:
        raise ValueError("feed response body is empty")
    if len(body) > FEED_MAX_RESPONSE_BYTES:
        raise ValueError("feed response exceeds the configured size limit")
    if _XML_FORBIDDEN.search(body):
        raise ValueError("feed XML declarations and entities are not allowed")
    try:
        root = ElementTree.fromstring(body)
    except ElementTree.ParseError as exc:
        raise ValueError("feed XML is malformed") from exc
    count = 0
    stack: list[tuple[ElementTree.Element, int]] = [(root, 1)]
    while stack:
        element, depth = stack.pop()
        count += 1
        if count > FEED_MAX_ELEMENTS:
            raise ValueError("feed XML contains too many elements")
        if depth > FEED_MAX_DEPTH:
            raise ValueError("feed XML is nested too deeply")
        stack.extend((child, depth + 1) for child in element)
    return root


def _safe_item_url(base_url: str, value: str | None) -> str | None:
    if not value:
        return None
    resolved = urlsplit(urljoin(base_url, value.strip()))
    if (
        resolved.scheme not in {"http", "https"}
        or not resolved.hostname
        or resolved.username is not None
        or resolved.password is not None
    ):
        return None
    return urlunsplit((resolved.scheme, resolved.netloc, resolved.path or "/", resolved.query, ""))


def _canonical_feed_url(value: str) -> str:
    parsed = urlsplit(value)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", parsed.query, ""))


def parse_rss(body: bytes, base_url: str) -> ParsedFeed:
    root = _parse_xml(body)
    root_name = _local_name(root.tag)
    if root_name == "rss":
        channel = _first_child(root, "channel")
    elif root_name == "rdf":
        channel = _first_child(root, "channel")
    else:
        channel = None
    if channel is None:
        raise ValueError("response is not an RSS feed")
    title = _element_text(_first_child(channel, "title"), limit=FEED_MAX_TITLE_CHARS)
    item_elements = _children(channel, "item")
    if root_name == "rdf":
        item_elements = _children(root, "item")
    if len(item_elements) > FEED_MAX_ITEMS:
        raise ValueError("RSS feed contains too many items")
    items: list[FeedItem] = []
    for item in item_elements:
        raw_id = _element_text(_first_child(item, "guid"), limit=FEED_MAX_IDENTIFIER_CHARS) or None
        raw_link = _element_text(_first_child(item, "link"), limit=FEED_MAX_IDENTIFIER_CHARS)
        item_title = _element_text(
            _first_child(item, "title"), limit=FEED_MAX_TITLE_CHARS
        ) or "Untitled item"
        published = _element_text(
            _first_child(item, "pubdate", "date"), limit=FEED_MAX_IDENTIFIER_CHARS
        ) or None
        content = _element_text(
            _first_child(item, "encoded", "description", "content"),
            limit=FEED_MAX_TEXT_CHARS,
        )
        items.append(
            FeedItem(
                source_id=raw_id,
                title=item_title,
                url=_safe_item_url(base_url, raw_link),
                published=published,
                content=content,
            )
        )
    return ParsedFeed(title or "RSS feed", tuple(items))


def parse_atom(body: bytes, base_url: str) -> ParsedFeed:
    root = _parse_xml(body)
    if _local_name(root.tag) != "feed":
        raise ValueError("response is not an Atom feed")
    entries = _children(root, "entry")
    if len(entries) > FEED_MAX_ITEMS:
        raise ValueError("Atom feed contains too many entries")
    title = _element_text(_first_child(root, "title"), limit=FEED_MAX_TITLE_CHARS)
    items: list[FeedItem] = []
    for entry in entries:
        alternate = next(
            (
                link
                for link in _children(entry, "link")
                if link.attrib.get("rel", "alternate").lower() == "alternate"
                and link.attrib.get("href")
            ),
            None,
        )
        raw_link = alternate.attrib.get("href") if alternate is not None else None
        raw_id = _element_text(_first_child(entry, "id"), limit=FEED_MAX_IDENTIFIER_CHARS) or None
        item_title = _element_text(
            _first_child(entry, "title"), limit=FEED_MAX_TITLE_CHARS
        ) or "Untitled entry"
        published = _element_text(
            _first_child(entry, "published", "updated"), limit=FEED_MAX_IDENTIFIER_CHARS
        ) or None
        content = _element_text(
            _first_child(entry, "content", "summary"), limit=FEED_MAX_TEXT_CHARS
        )
        items.append(
            FeedItem(
                source_id=raw_id,
                title=item_title,
                url=_safe_item_url(base_url, raw_link),
                published=published,
                content=content,
            )
        )
    return ParsedFeed(title or "Atom feed", tuple(items))


def _feed_url(context: ConnectorOperationContext) -> str:
    if context.binding is None:
        raise ValueError("feed connector binding is missing")
    value = context.binding.metadata.get("feed_url")
    if not isinstance(value, str) or not value:
        raise ValueError("feed connector URL is missing")
    return value


def _resource_external_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _item_external_id(item: FeedItem) -> str:
    source = item.source_id or item.url
    if source is None:
        source = json.dumps(
            [item.title, item.published, item.content],
            ensure_ascii=False,
            separators=(",", ":"),
        )
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def _item_revision(item: FeedItem) -> str:
    encoded = json.dumps(
        [item.source_id, item.title, item.url, item.published, item.content],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _seen_ids(cursor: ConnectorCursor | None) -> tuple[str, ...]:
    if cursor is None:
        return ()
    try:
        parsed = json.loads(cursor.value)
    except ValueError:
        return ()
    if not isinstance(parsed, dict) or not isinstance(parsed.get("seen"), list):
        return ()
    values = [
        value
        for value in parsed["seen"]
        if isinstance(value, str) and len(value) == 64
    ]
    return tuple(values[:FEED_MAX_SEEN_IDS])


def _cursor_value(current: list[str], previous: tuple[str, ...]) -> str:
    ordered = list(dict.fromkeys([*current, *previous]))[:FEED_MAX_SEEN_IDS]
    return json.dumps({"seen": ordered}, separators=(",", ":"), sort_keys=True)


class FeedProvider(BaseConnectorProvider):
    feed_kind: str
    parser: FeedParser

    def __init__(self, http_client: ConnectorHttpClient | None = None) -> None:
        self._http = http_client or ConnectorHttpClient(
            ConnectorHttpPolicy(
                timeout_seconds=10,
                max_redirects=3,
                max_response_bytes=FEED_MAX_RESPONSE_BYTES,
            )
        )

    async def setup(
        self, context: ConnectorOperationContext, values: dict[str, str]
    ) -> ConnectorSetupResult:
        url = values["url"].strip()
        response = await self._http.get_response(url)
        final_url = _canonical_feed_url(response.final_url)
        parsed = self.parser(response.body, final_url)
        return ConnectorSetupResult(
            ConnectorBindingDraft(
                display_name=parsed.title,
                metadata={"feed_url": final_url, "feed_title": parsed.title},
            )
        )

    async def list_resources(
        self, context: ConnectorOperationContext
    ) -> ConnectorResourceResult:
        response = await self._http.get_response(_feed_url(context))
        final_url = _canonical_feed_url(response.final_url)
        parsed = self.parser(response.body, final_url)
        return ConnectorResourceResult(
            (
                ConnectorResourceDraft(
                    external_id=_resource_external_id(final_url),
                    kind=f"{self.feed_kind}_feed",
                    display_name=parsed.title,
                    url=final_url,
                    selected=True,
                    config={"format": self.feed_kind},
                ),
            )
        )

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        if context.binding is None:
            raise ValueError("feed connector binding is missing")
        changes: list[ConnectorChange] = []
        cursor_updates: list[ConnectorCursorUpdate] = []
        for resource in context.resources:
            url = resource.url or _feed_url(context)
            cursor = next(
                (
                    item
                    for item in context.cursors
                    if item.resource_id == resource.id and item.stream == _CURSOR_STREAM
                ),
                None,
            )
            request_headers: dict[str, str] = {}
            if cursor is not None and cursor.etag:
                request_headers["If-None-Match"] = cursor.etag
            if cursor is not None and cursor.last_modified:
                request_headers["If-Modified-Since"] = cursor.last_modified
            response = await self._http.get_response(
                url,
                request_headers=request_headers,
                accepted_status_codes=frozenset({200, 304}),
            )
            etag = response.headers.get("etag") or (cursor.etag if cursor else None)
            last_modified = response.headers.get("last-modified") or (
                cursor.last_modified if cursor else None
            )
            if response.status_code == 304:
                cursor_updates.append(
                    ConnectorCursorUpdate(
                        _CURSOR_STREAM,
                        cursor.value if cursor is not None else _cursor_value([], ()),
                        resource_id=resource.id,
                        etag=etag,
                        last_modified=last_modified,
                        revision=cursor.revision if cursor else None,
                    )
                )
                continue
            final_url = _canonical_feed_url(response.final_url)
            parsed = self.parser(response.body, final_url)
            previous = _seen_ids(cursor)
            seen = set(previous)
            current_ids: list[str] = []
            for item in parsed.items:
                external_id = _item_external_id(item)
                current_ids.append(external_id)
                if external_id in seen:
                    continue
                seen.add(external_id)
                revision = _item_revision(item)
                provenance = ConnectorProvenance(
                    connector_id=self.manifest.id,
                    binding_id=context.binding.id,
                    external_resource_id=external_id,
                    source_url=item.url or final_url,
                    revision=revision,
                    event_id=external_id,
                )
                event = ConnectorEvent(
                    type=f"{self.feed_kind}.item",
                    provenance=provenance,
                    payload={
                        "feed": {
                            "title": parsed.title,
                            "url": final_url,
                        },
                        "item": {
                            "id": external_id,
                            "source_id": item.source_id,
                            "title": item.title,
                            "url": item.url,
                            "published": item.published,
                            "content": item.content,
                        },
                    },
                )
                changes.append(
                    ConnectorChange(
                        ConnectorChangeKind.event,
                        provenance,
                        event=event,
                    )
                )
            body_revision = hashlib.sha256(response.body).hexdigest()
            cursor_updates.append(
                ConnectorCursorUpdate(
                    _CURSOR_STREAM,
                    _cursor_value(current_ids, previous),
                    resource_id=resource.id,
                    etag=etag,
                    last_modified=last_modified,
                    revision=body_revision,
                )
            )
        return ConnectorSyncResult(
            changes=tuple(changes),
            state=ConnectorStateUpdate(cursor_updates=tuple(cursor_updates)),
        )

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        if context.binding is None:
            return ConnectorHealth(
                ConnectorHealthStatus.unconfigured,
                datetime.now(UTC),
                "Feed connector is not configured.",
            )
        try:
            _feed_url(context)
        except ValueError as exc:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                datetime.now(UTC),
                str(exc),
            )
        return ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC))


__all__ = [
    "FEED_POLL_CADENCE_SECONDS",
    "FeedProvider",
    "ParsedFeed",
    "parse_atom",
    "parse_rss",
    "sanitize_feed_text",
]
