import asyncio
from typing import List

from hawk.callback import QueueCallback
from hawk.connection import Connection


class Consumer:

    def __init__(self, connection: Connection):
        self.connection = connection
        self._callbacks = []  # type: List[QueueCallback]
        self.loop = asyncio.get_event_loop()

    def add_consumer_callback(self, callback: QueueCallback):
        self._callbacks.append(callback)

    async def connect(self):
        await self.connection.connect()
        for handler in self._callbacks:
            handler.connection = self.connection.connection

    async def close(self):
        await self.connection.close()

    @staticmethod
    async def consume_from_queue(queue_handler: QueueCallback):
        async with queue_handler:
            async with queue_handler.queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        await queue_handler.handle_message(message)

    async def consume(self) -> None:
        await self.connect()
        tasks = [asyncio.create_task(self.consume_from_queue(queue_handler)) for queue_handler in self._callbacks]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in pending:
            task.cancel()

        await self.close()
