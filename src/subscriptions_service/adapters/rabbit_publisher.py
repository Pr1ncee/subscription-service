import asyncio

from aiormq.exceptions import AMQPConnectionError, ChannelInvalidStateError
from faststream.rabbit import RabbitBroker

from subscriptions_service.adapters.rabbit_topology import EVENTS_EXCHANGE
from subscriptions_service.domain.events import EventEnvelope
from subscriptions_service.services.ports import PublisherUnavailable


class RabbitPublisher:
    """`EventPublisher` over RabbitMQ: persistent, mandatory, publisher-confirmed.

    The broker must be created with `Channel(on_return_raises=True)` so that an
    unroutable message raises instead of being silently dropped; a nack raises too.
    Transport failures (timeout, lost connection/channel) become `PublisherUnavailable`.
    """

    def __init__(self, broker: RabbitBroker, timeout_s: float) -> None:
        self._broker = broker
        self._timeout_s = timeout_s

    async def publish(self, envelope: EventEnvelope) -> None:
        try:
            # The outer timeout also bounds the wait for a reconnecting channel,
            # which the confirm timeout passed to the broker does not cover.
            async with asyncio.timeout(self._timeout_s):
                await self._broker.publish(
                    envelope.to_json(),
                    exchange=EVENTS_EXCHANGE,
                    routing_key=envelope.event_type.value,
                    mandatory=True,
                    persist=True,
                    message_id=str(envelope.event_id),
                    content_type="application/json",
                    timeout=self._timeout_s,
                )
        except TimeoutError as e:
            raise PublisherUnavailable(f"publish not confirmed within {self._timeout_s}s") from e
        except (AMQPConnectionError, ChannelInvalidStateError, ConnectionError) as e:
            raise PublisherUnavailable(f"broker connection unavailable: {e}") from e
