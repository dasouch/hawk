import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection

from hawk.callback import QueueCallback
from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST


class Consumer:

    def __init__(self, prefetch_count: int = 100):
        self._callbacks = {}
        self.loop = asyncio.get_event_loop()
        self._connection = None
        self._channel = None
        self._prefetch_count = prefetch_count

    def add_consumer_callback(self, queue_name, callback: QueueCallback):
        self._callbacks[queue_name] = callback

    @staticmethod
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust(
            f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/{RABBIT_VIRTUAL_HOST}")

    async def consume(self):
        self._connection = await self.get_connection()
        async with self._connection:
            channel = await self._connection.channel()
            await channel.set_qos(prefetch_count=self._prefetch_count)
            for queue_name, queue_handler in self._callbacks.items():
                queue = await channel.declare_queue(queue_name, auto_delete=False, durable=True)
                await queue.consume(queue_handler.handle_message)

            while True:
                await asyncio.sleep(1)
