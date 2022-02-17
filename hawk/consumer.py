import asyncio
import logging

import aio_pika
import ujson

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST

logger = logging.getLogger('hawk')


class Consumer:

    def __init__(self):
        self._connection = None

    async def __aenter__(self):
        try:
            self._connection = await aio_pika.connect_robust(
                f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/{RABBIT_VIRTUAL_HOST}",
                loop=asyncio.get_event_loop(), timeout=3)
        except Exception as error:
            logger.debug(f'error connection with Rabbit', extra={'error': error})
        return self

    async def consume(self, queue_name, callback):
        if self._connection:
            async with self._connection.channel() as channel:
                queue = await channel.declare_queue(
                    queue_name,
                    auto_delete=False,
                    durable=True
                )
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            body = ujson.loads(message.body)
                            logger.debug('consuming messages from queue', extra={'queue': queue_name, 'body': body})
                            await callback(body)
                            logger.debug('callback done from queue', extra={'queue': queue_name})

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            await self._connection.close(None)
