import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST


class Consumer:

    def __init__(self):
        self._callbacks = {}
        self.loop = asyncio.get_event_loop()
        self._connection = None
        self._channel = None

    def add_consumer_callback(self, queue_name, callback):
        self._callbacks[queue_name] = callback

    @staticmethod
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust(
            f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/{RABBIT_VIRTUAL_HOST}")

    async def consume(self):
        self._connection = await self.get_connection()
        async with self._connection:
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=100)
            tasks = []
            for queue_name, callback in self._callbacks.items():
                tasks.append(asyncio.create_task(self._consumer(queue_name=queue_name, callback=callback)))
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _consumer(self, queue_name, callback):
        queue = await self._channel.declare_queue(queue_name, durable=True, auto_delete=False)
        await queue.consume(callback=callback)

        try:
            await asyncio.Future()
        finally:
            await self._connection.close()
