import abc
import asyncio

import aio_pika
import ujson


class QueueCallback:
    def __init__(self, queue_name, prefetch_count: int = 100):
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.connection = None
        self.channel_closed_event = asyncio.Event()

    async def __aenter__(self):
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.prefetch_count)
        self.queue = await self.channel.declare_queue(self.queue_name, auto_delete=False, durable=True)
        self.queue.channel.close_callbacks.add(lambda channel, exception: self.channel_closed_event.set())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.channel.close()

    @abc.abstractmethod
    async def handle(self, message: dict):
        raise NotImplementedError

    async def handle_message(self, message: aio_pika.IncomingMessage):
        try:
            async with message.process():
                await self.handle(message=ujson.loads(message.body))
        except Exception as exc:
            await self.fallback(exc)

    @abc.abstractmethod
    async def fallback(self, exc):
        pass
