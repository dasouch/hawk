import asyncio

import aio_pika
import ujson
from aio_pika.abc import AbstractRobustConnection

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST


class Consumer:

    def __init__(self):
        self._callbacks = {}
        self._connection = None
        self.loop = asyncio.get_event_loop()

    def add_consumer_callback(self, queue_name, callback):
        self._callbacks[queue_name] = callback

    @staticmethod
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust(
            f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/{RABBIT_VIRTUAL_HOST}")

    async def consume(self):
        tasks = []
        for queue_name, callback in self._callbacks.items():
            tasks.append(self._consumer(queue_name=queue_name, callback=callback))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _consumer(self, queue_name, callback):
        connection = await self.get_connection()
        async with connection:
            async with connection.channel() as channel:
                queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)
                async with queue.iterator() as queue_iter:
                    try:
                        async for message in queue_iter:
                            async with message.process():
                                body = ujson.loads(message.body)
                                await callback(body)
                    except asyncio.CancelledError:
                        pass
