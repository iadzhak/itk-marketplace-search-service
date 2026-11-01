import logging
import re

from httpx import AsyncClient

from src.infrastructure.tracing import TRACE_ID_HEADER, get_trace_id

UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
URL = "/search?q=none"


async def test_generates_trace_id_when_header_missing(client: AsyncClient) -> None:
    resp = await client.get(URL)

    assert UUID_PATTERN.match(resp.headers[TRACE_ID_HEADER])


async def test_echoes_incoming_trace_id(client: AsyncClient) -> None:
    resp = await client.get(URL, headers={TRACE_ID_HEADER: "abc-123"})

    assert resp.headers[TRACE_ID_HEADER] == "abc-123"


async def test_replaces_invalid_trace_id(client: AsyncClient) -> None:
    resp = await client.get(URL, headers={TRACE_ID_HEADER: "bad value\twith spaces"})

    assert UUID_PATTERN.match(resp.headers[TRACE_ID_HEADER])


async def test_replaces_too_long_trace_id(client: AsyncClient) -> None:
    resp = await client.get(URL, headers={TRACE_ID_HEADER: "a" * 65})

    assert UUID_PATTERN.match(resp.headers[TRACE_ID_HEADER])


async def test_trace_id_reset_after_request(client: AsyncClient) -> None:
    await client.get(URL, headers={TRACE_ID_HEADER: "abc-123"})

    assert get_trace_id() is None

