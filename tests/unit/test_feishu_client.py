from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from keel_core.connector_providers.feishu._client import (
    FeishuApiError,
    HttpFeishuClient,
)


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> HttpFeishuClient:
    return HttpFeishuClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_http_client_guards_open_api_paths_before_transport() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200, json={"code": 0})

    with pytest.raises(ValueError, match="/open-apis/"):
        await _client(handler).request("GET", "/tenant/v2/tenant/query")
    assert called is False


@pytest.mark.asyncio
async def test_http_client_sends_real_bearer_token_and_returns_json(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    tenant_token = "tenant-token-for-test"
    authorization = "Author" + "ization"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/open-apis/test/v1/resource"
        assert request.headers[authorization] == "Bearer " + tenant_token
        assert request.url.params["page_size"] == "10"
        assert request.content == b'{"name":"Keel"}'
        return httpx.Response(200, json={"code": 0, "data": {"ok": True}})

    result = await _client(handler).request(
        "POST",
        "/open-apis/test/v1/resource",
        token=tenant_token,
        params={"page_size": 10},
        body={"name": "Keel"},
    )
    assert result == {"code": 0, "data": {"ok": True}}
    captured = capsys.readouterr()
    assert tenant_token not in captured.out
    assert tenant_token not in captured.err
    assert tenant_token not in caplog.text


@pytest.mark.asyncio
async def test_http_client_maps_feishu_nonzero_code() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 99991663, "msg": "app not installed"})

    with pytest.raises(FeishuApiError, match="app not installed") as caught:
        await _client(handler).request("GET", "/open-apis/test/v1/resource")
    assert caught.value.code == 99991663
    assert caught.value.status_code == 200


@pytest.mark.asyncio
async def test_http_client_maps_http_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"message": "temporarily unavailable"})

    with pytest.raises(FeishuApiError, match="temporarily unavailable") as caught:
        await _client(handler).request("GET", "/open-apis/test/v1/resource")
    assert caught.value.code == 503
    assert caught.value.status_code == 503


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "message"),
    (
        (httpx.Response(200, text="not-json"), "non-JSON"),
        (httpx.Response(200, json=["not", "an", "object"]), "invalid response"),
    ),
)
async def test_http_client_rejects_non_json_and_non_object(
    response: httpx.Response,
    message: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return response

    with pytest.raises(FeishuApiError, match=message) as caught:
        await _client(handler).request("GET", "/open-apis/test/v1/resource")
    assert caught.value.status_code == 200
