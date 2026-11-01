import logging
from dataclasses import dataclass
from typing import Any

from src.application.services.kafka_ads_consumer import (
    KafkaAdsConsumer,
    extract_trace_id,
)
from src.infrastructure.tracing import get_trace_id, trace_context


@dataclass
class FakeMessage:
    value: dict[str, Any]
    headers: list[tuple[str, bytes]] | None
    partition: int = 0
    offset: int = 7


class FakeKafkaConsumer:
    def __init__(self, messages: list[FakeMessage]) -> None:
        self._messages = messages
        self.commits = 0

    def __aiter__(self) -> "FakeKafkaConsumer":
        self._iter = iter(self._messages)
        return self

    async def __anext__(self) -> FakeMessage:
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration

    async def commit(self) -> None:
        self.commits += 1


class RecordingUseCase:
    def __init__(self, fail: bool = False) -> None:
        self.trace_ids: list[str | None] = []
        self._fail = fail

    async def execute(self, ad_id: int) -> None:
        self.trace_ids.append(get_trace_id())
        if self._fail:
            raise RuntimeError("boom")


def _message(headers: list[tuple[str, bytes]] | None, event: str = "ad.created"):
    return FakeMessage(value={"event": event, "payload": {"ad_id": 1}}, headers=headers)


def test_extract_trace_id_is_case_insensitive() -> None:
    assert extract_trace_id([("X-Trace-Id", b"abc")]) == "abc"


def test_extract_trace_id_missing() -> None:
    assert extract_trace_id([("other", b"1")]) is None
    assert extract_trace_id(None) is None


async def test_consumer_sets_trace_id_from_header() -> None:
    index_ad = RecordingUseCase()
    kafka = FakeKafkaConsumer([_message([("x-trace-id", b"trace-k1")])])

    await KafkaAdsConsumer(kafka, index_ad, RecordingUseCase(), trace_context).run()

    assert index_ad.trace_ids == ["trace-k1"]
    assert kafka.commits == 1
    assert get_trace_id() is None


async def test_consumer_generates_trace_id_without_header() -> None:
    index_ad = RecordingUseCase()
    kafka = FakeKafkaConsumer([_message(None), _message([])])

    await KafkaAdsConsumer(kafka, index_ad, RecordingUseCase(), trace_context).run()

    assert len(index_ad.trace_ids) == 2
    assert all(t and len(t) == 36 for t in index_ad.trace_ids)
    assert index_ad.trace_ids[0] != index_ad.trace_ids[1]


async def test_consumer_remove_event_uses_header_trace_id() -> None:
    remove_ad = RecordingUseCase()
    kafka = FakeKafkaConsumer(
        [_message([("x-trace-id", b"trace-del")], event="ad.deleted")]
    )

    await KafkaAdsConsumer(kafka, RecordingUseCase(), remove_ad, trace_context).run()

    assert remove_ad.trace_ids == ["trace-del"]


async def test_failure_is_logged_with_trace_id_and_not_committed(caplog) -> None:
    caplog.set_level(logging.ERROR)
    kafka = FakeKafkaConsumer([_message([("x-trace-id", b"trace-fail")])])

    await KafkaAdsConsumer(
        kafka, RecordingUseCase(fail=True), RecordingUseCase(), trace_context
    ).run()

    assert kafka.commits == 0
    assert caplog.records[0].trace_id == "trace-fail"


async def test_consumer_logs_received_message_with_trace_id(caplog) -> None:
    caplog.set_level(logging.INFO)
    kafka = FakeKafkaConsumer([_message([("x-trace-id", b"trace-recv")])])

    await KafkaAdsConsumer(
        kafka, RecordingUseCase(), RecordingUseCase(), trace_context
    ).run()

    records = [r for r in caplog.records if r.getMessage().startswith("consumed")]
    assert records[0].getMessage() == "consumed ad.created ad_id=1 partition=0 offset=7"
    assert records[0].trace_id == "trace-recv"
