import asyncio
import logging
from typing import Dict

import aio_pika
import ujson

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST

logger = logging.getLogger('hawk')


class Publisher:

    @classmethod
    async def send_message(cls, queue_name, data: Dict):
        logger.debug(f'creating send queue {queue_name}', extra=data)
        connection = await aio_pika.connect_robust(f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/",
                                                   loop=asyncio.get_event_loop())
        async with connection:
            channel = await connection.channel()
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
