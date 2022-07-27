from pytest import mark

from hawk import Publisher
from hawk.consumer import Consumer


@mark.asyncio
async def test_consumer_success(event_loop):
    async with Publisher() as publisher:
        await publisher.send_message('test_queue', data={'first_name': 'Danilo', 'last_name': 'Vargas'})

    async def queue_callback(data):
        print('data: ', data)

    consumer = Consumer()
    consumer.add_consumer_callback(queue_name='test_queue', callback=queue_callback)
    consumer.add_consumer_callback(queue_name='one_queue', callback=queue_callback)
    await consumer.consume()
