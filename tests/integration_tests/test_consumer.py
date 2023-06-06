from pytest import mark

from hawk import Publisher, QueueCallback
from hawk.connection import Connection
from hawk.consumer import Consumer


@mark.asyncio
async def test_consumer_success(event_loop):
    connection = Connection(user='guest', password='guest', host='127.0.0.1')
    async with Publisher(connection=connection) as publisher:
        await publisher.send_message('test_queue', data={'first_name': 'Danilo', 'last_name': 'Vargas'})
        await publisher.send_message('one_queue', data={'first_name': 'Danilo', 'last_name': 'Vargas'})

    class TestQueueCallback(QueueCallback):
        async def fallback(self, exc):
            pass

        async def handle(self, message: dict):
            assert message

    class TestOneQueueCallback(QueueCallback):
        async def fallback(self, exc):
            pass

        async def handle(self, message: dict):
            assert message

    consumer = Consumer(connection=connection)
    consumer.add_consumer_callback(callback=TestQueueCallback(queue_name='test_queue'))
    consumer.add_consumer_callback(callback=TestOneQueueCallback(queue_name='one_queue'))
    await consumer.consume()
