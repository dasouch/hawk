from pytest import mark

from hawk.consumer import Consumer


@mark.asyncio
async def test_consumer_success(event_loop):
    def test(data):
        pass

    await Consumer.consume('test_queue', callback=test)
