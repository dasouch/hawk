from pytest import mark

from hawk.publisher import Publisher


@mark.asyncio
async def test_publisher_success(event_loop):
    await Publisher.send_message('test_queue', data={'first_name': 'Danilo', 'last_name': 'Vargas'})
