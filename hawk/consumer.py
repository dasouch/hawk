import asyncio
from typing import AnyStr

import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractRobustConnection

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST


class Consumer:

    def __init__(self, service: AnyStr):
        self._callbacks = {}
        self.loop = asyncio.get_event_loop()
        self._service = service

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
        await asyncio.gather(*tasks)

    async def _consumer(self, queue_name, callback):
        connection = await self.get_connection()
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        exchange = await channel.declare_exchange(self._service, ExchangeType.FANOUT)
        queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)
        await queue.bind(exchange)
        await queue.consume(callback=callback)
        try:
            await asyncio.Future()
        finally:
            await connection.close()
