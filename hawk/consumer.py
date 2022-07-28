import asyncio
import logging

import aio_pika
import ujson
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST

logger = logging.getLogger('hawk')


class Consumer:

    def __init__(self):
        self._callbacks = {}
        self._connection = None
        self.loop = asyncio.get_event_loop()
        self.connection_pool: Pool = Pool(self.get_connection, max_size=2, loop=self.loop)
        self.channel_pool: Pool = Pool(self.get_channel, max_size=10, loop=self.loop)

    def add_consumer_callback(self, queue_name, callback):
        self._callbacks[queue_name] = callback

    @staticmethod
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust(
            f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/{RABBIT_VIRTUAL_HOST}")

    async def get_channel(self) -> aio_pika.Channel:
        async with self.connection_pool.acquire() as connection:
            return await connection.channel()

    async def consume(self):
        async with self.connection_pool, self.channel_pool:
            tasks = []
            for queue_name, callback in self._callbacks.items():
                tasks.append(self._consumer(queue_name=queue_name, callback=callback))
            await asyncio.gather(*tasks)

    async def _consumer(self, queue_name, callback):
        async with self.channel_pool.acquire() as channel:  # type: aio_pika.Channel
            queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        body = ujson.loads(message.body)
                        logger.debug('consuming messages from queue', extra={'queue': queue_name, 'body': body})
                        await callback(body)
                        logger.debug('callback done from queue', extra={'queue': queue_name})
