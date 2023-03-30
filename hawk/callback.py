import abc

import aio_pika
import ujson


class QueueCallback:

    @abc.abstractmethod
    async def handle(self, message: dict):
        raise NotImplementedError

    async def handle_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            message = ujson.loads(message.body)
            await self.handle(message=message)