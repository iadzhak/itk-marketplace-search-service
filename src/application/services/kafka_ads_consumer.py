import logging
import typing

from aiokafka import AIOKafkaConsumer

from src.application.ports.trace_scope import TraceScope
from src.application.ports.usecases import IndexAdPort, RemoveAdPort

logger = logging.getLogger(__name__)

TRACE_ID_KAFKA_HEADER = "x-trace-id"


def extract_trace_id(headers: typing.Iterable[tuple[str, bytes]] | None) -> str | None:
    for key, value in headers or ():
        if key.lower() == TRACE_ID_KAFKA_HEADER and value is not None:
            return value.decode("utf-8", errors="replace")
    return None


class KafkaAdsConsumer:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        index_ad: IndexAdPort,
        remove_ad: RemoveAdPort,
        trace_scope: TraceScope,
    ) -> None:
        self._consumer = consumer
        self._index_ad = index_ad
        self._remove_ad = remove_ad
        self._trace_scope = trace_scope

    async def run(self) -> None:
        async for msg in self._consumer:
            with self._trace_scope(extract_trace_id(msg.headers)):
                logger.info(
                    "consumed %s ad_id=%s partition=%s offset=%s",
                    msg.value.get("event"),
                    (msg.value.get("payload") or {}).get("ad_id"),
                    msg.partition,
                    msg.offset,
                )
                try:
                    await self._handle(msg.value)
                except Exception:
                    logger.exception("failed to handle message %s", msg)
                    continue
                await self._consumer.commit()

    async def _handle(self, value: dict[str, typing.Any]) -> None:
        event = value.get("event")
        payload = value.get("payload") or {}
        ad_id = payload.get("ad_id")
        if not isinstance(ad_id, int):
            logger.warning("skip message without ad_id: %s", value)
            return

        if event in ("ad.created", "ad.updated"):
            await self._index_ad.execute(ad_id)
        elif event == "ad.deleted":
            await self._remove_ad.execute(ad_id)
        else:
            logger.warning("unknown event type: %s", event)
