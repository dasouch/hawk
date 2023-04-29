import asyncio

from hawk.callback import QueueCallback


class Consumer:

    def __init__(self):
        self._callbacks = []
        self.loop = asyncio.get_event_loop()

    def add_consumer_callback(self, callback: QueueCallback):
        self._callbacks.append(callback)

    @staticmethod
    async def consume_from_queue(queue_handler: QueueCallback):
        async with queue_handler:
            await queue_handler.queue.consume(queue_handler.handle_message)
            await asyncio.Future()

    async def consume(self) -> None:
        tasks = [asyncio.create_task(self.consume_from_queue(queue_handler)) for queue_handler in self._callbacks]
        await asyncio.gather(*tasks)
