from pytest import mark

from hawk import Publisher, QueueCallback
from hawk.consumer import Consumer


@mark.asyncio
async def test_consumer_success(event_loop):
    async with Publisher() as publisher:
        await publisher.send_message('test_queue', data={'first_name': 'Danilo', 'last_name': 'Vargas'})
        await publisher.send_message('one_queue', data={'first_name': 'Danilo', 'last_name': 'Vargas'})

    class TestQueueCallback(QueueCallback):
        async def handle(self, message: dict):
            assert message

    class TestOneQueueCallback(QueueCallback):
        async def handle(self, message: dict):
            assert message

    consumer = Consumer()
    consumer.add_consumer_callback(queue_name='test_queue', callback=TestQueueCallback())
    consumer.add_consumer_callback(queue_name='one_queue', callback=TestOneQueueCallback())
    await consumer.consume()
