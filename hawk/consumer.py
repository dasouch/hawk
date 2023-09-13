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
            await queue_handler.queue.consume(queue_handler.handle_message)
            await queue_handler.channel_closed_event.wait()

    async def consume(self) -> None:
        await self.connect()
        tasks = [asyncio.create_task(self.consume_from_queue(queue_handler)) for queue_handler in self._callbacks]
        await asyncio.gather(*tasks)
        await self.close()
