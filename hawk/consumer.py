import asyncio
import logging

import aio_pika
import ujson

from hawk.settings import RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST

logger = logging.getLogger('hawk')


class Consumer:

    @classmethod
    async def consume(cls, queue_name, callback):
        connection = await aio_pika.connect_robust(
            f'amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_HOST}/', loop=asyncio.get_event_loop()
        )

        async with connection:
            channel = await connection.channel()
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
