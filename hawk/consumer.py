import asyncio

import aio_pika
import ujson
from aio_pika.abc import AbstractRobustConnection

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST


class Consumer:

    def __init__(self):
        self._callbacks = {}
        self.loop = asyncio.get_event_loop()
        self._channel = None

    def add_consumer_callback(self, queue_name, callback):
        self._callbacks[queue_name] = callback

    @staticmethod
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust(
            f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/{RABBIT_VIRTUAL_HOST}")

    async def consume(self):
        _connection = await self.get_connection()
        async with _connection:
            self._channel = await _connection.channel()
            await self._channel.set_qos(prefetch_count=10)
            tasks = []
            for queue_name, callback in self._callbacks.items():
                tasks.append(self._consumer(queue_name=queue_name, callback=callback))
            await asyncio.gather(*tasks)

    async def _consumer(self, queue_name, callback):
        queue = await self._channel.declare_queue(queue_name, durable=True, auto_delete=False)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    await callback(ujson.loads(message.body))
