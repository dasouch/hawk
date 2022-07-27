import asyncio
import logging

import aio_pika
import ujson

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VIRTUAL_HOST

logger = logging.getLogger('hawk')


class Consumer:

    def __init__(self):
        self._callbacks = {}
        self.channel = None

    def add_consumer_callback(self, queue_name, callback):
        self._callbacks[queue_name] = callback

    async def consume(self):
        _connection = await aio_pika.connect_robust(
            f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/{RABBIT_VIRTUAL_HOST}")
        async with _connection:
            self.channel = await _connection.channel()
            await self.channel.set_qos(prefetch_count=100)
            tasks = []
            for queue_name, callback in self._callbacks.items():
                tasks.append(self._consumer(queue_name=queue_name, callback=callback))
            await asyncio.gather(*tasks)

    async def _consumer(self, queue_name, callback):
        queue = await self.channel.declare_queue(
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
