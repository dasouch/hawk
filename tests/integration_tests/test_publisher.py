from pytest import mark

from hawk.connection import Connection
from hawk.publisher import Publisher


@mark.asyncio
async def test_publisher_success(event_loop):
    connection = Connection(user='guest', password='guest', host='127.0.0.1')
    async with Publisher(connection=connection) as publisher:
        await publisher.send_message('test_queue', data={'first_name': 'Danilo', 'last_name': 'Vargas'})
