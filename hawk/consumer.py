import asyncio
import logging

import aio_pika
import ujson

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST


class Consumer:

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self._connection = None
        self.logger = logging.getLogger('hawk')

    async def __aenter__(self):
        try:
            self._connection = await aio_pika.connect_robust(
                f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/{RABBIT_VIRTUAL_HOST}",
                loop=asyncio.get_event_loop())
        except Exception as error:
            self.logger.error(f"error connection with Rabbit", exc_info=error)
        return self

    async def consume(self, callback, queue_name):
        _channel = await self._connection.channel()
        await _channel.set_qos(prefetch_count=10)
        queue = await _channel.declare_queue(queue_name, durable=True, auto_delete=False)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    await callback(ujson.loads(message.body))

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            await self._connection.close(None)
