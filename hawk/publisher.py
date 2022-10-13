import asyncio
import logging
from typing import Dict, AnyStr

import aio_pika
import ujson
from aio_pika import ExchangeType, Message, DeliveryMode

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST

logger = logging.getLogger('hawk')


class Publisher:

    def __init__(self, service: AnyStr):
        self._connection = None
        self._service = service

    async def __aenter__(self):
        try:
            self._connection = await aio_pika.connect_robust(
                f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/{RABBIT_VIRTUAL_HOST}",
                loop=asyncio.get_event_loop())
        except Exception as error:
            logger.debug(f'error connection with Rabbit', extra={'error': error})
        return self

    async def send_message(self, queue_name, data: Dict):
        logger.debug(f'creating send queue {queue_name}', extra=data)
        if self._connection:
            async with self._connection.channel() as channel:
                await channel.declare_queue(
                    queue_name,
                    auto_delete=False,
                    durable=True
                )
                await channel.default_exchange.publish(
                    aio_pika.Message(body=ujson.dumps(data).encode()),
                    routing_key=queue_name
                )
                logger.debug(f'success send queue {queue_name}', extra=data)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            await self._connection.close(None)
