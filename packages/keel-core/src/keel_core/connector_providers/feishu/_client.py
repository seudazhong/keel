"""Small, dependency-light Feishu Open Platform client."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx

FEISHU_API_BASE = "https://open.feishu.cn"


class FeishuApiError(RuntimeError):
    def __init__(self, code: int, message: str, *, status_code: int = 200) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


class FeishuClient(Protocol):
    async def request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: Mapping[str, str | int] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class HttpFeishuClient:
    base_url: str = FEISHU_API_BASE
    timeout_seconds: float = 15.0
    transport: httpx.AsyncBaseTransport | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    async def request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: Mapping[str, str | int] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not path.startswith("/open-apis/"):
            raise ValueError("Feishu API path must stay under /open-apis/")
        headers = {"Authorization": "Bearer " + token} if token else {}
        async with httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            follow_redirects=False,
            transport=self.transport,
        ) as client:
            response = await client.request(
                method,
                path,
                params=dict(params or {}),
                json=dict(body) if body is not None else None,
                headers=headers,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise FeishuApiError(
                response.status_code,
                "Feishu returned a non-JSON response",
                status_code=response.status_code,
            ) from exc
        if not isinstance(payload, dict):
            raise FeishuApiError(
                response.status_code,
                "Feishu returned an invalid response",
                status_code=response.status_code,
            )
        code = payload.get("code", 0 if response.is_success else response.status_code)
        if not isinstance(code, int):
            code = response.status_code
        if not response.is_success or code != 0:
            message = str(
                payload.get("msg") or payload.get("message") or "Feishu API request failed"
            )
            raise FeishuApiError(code, message, status_code=response.status_code)
        return payload


async def paginate(
    client: FeishuClient,
    path: str,
    *,
    token: str,
    params: Mapping[str, str | int] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query: dict[str, str | int] = dict(params or {})
    query.setdefault("page_size", limit)
    rows: list[dict[str, Any]] = []
    page_token = ""
    for _ in range(100):
        if page_token:
            query["page_token"] = page_token
        payload = await client.request("GET", path, token=token, params=query)
        data = payload.get("data")
        if not isinstance(data, dict):
            return rows
        items = data.get("items")
        if isinstance(items, list):
            rows.extend(item for item in items if isinstance(item, dict))
        has_more = data.get("has_more") is True
        next_token = data.get("page_token")
        if not has_more or not isinstance(next_token, str) or not next_token:
            return rows
        page_token = next_token
    raise FeishuApiError(-1, "Feishu pagination exceeded 100 pages")


__all__ = [
    "FEISHU_API_BASE",
    "FeishuApiError",
    "FeishuClient",
    "HttpFeishuClient",
    "paginate",
]
