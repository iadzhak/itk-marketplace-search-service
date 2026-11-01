import httpx

from src.infrastructure.tracing import TRACE_ID_HEADER, trace_context
from src.infrastructure.http.trace_hook import inject_trace_id


async def _request_headers(inside_context: bool) -> httpx.Headers:
    captured: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.headers)
        return httpx.Response(200)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        event_hooks={"request": [inject_trace_id]},
    ) as client:
        if inside_context:
            with trace_context("trace-http"):
                await client.get("http://ad/internal/ads/1")
        else:
            await client.get("http://ad/internal/ads/1")
    return captured[0]


async def test_http_hook_adds_trace_id_header() -> None:
    headers = await _request_headers(inside_context=True)

    assert headers[TRACE_ID_HEADER] == "trace-http"


async def test_http_hook_skips_header_without_trace_id() -> None:
    headers = await _request_headers(inside_context=False)

    assert TRACE_ID_HEADER not in headers
