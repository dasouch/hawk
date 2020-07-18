from pytest import mark

from hawk.consumer import Consumer


@mark.asyncio
async def test_consumer_success(event_loop):
    async def test(data):
        pass

    async with Consumer() as consumer:
        await consumer.consume('test_queue', callback=test)
