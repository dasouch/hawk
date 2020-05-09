import asyncio
from unittest.mock import MagicMock

from pytest import fixture

from hawk.consumer import Consumer


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


@fixture(autouse=True, scope='function')
async def mocks(mocker):
    mocker.patch.object(Consumer, 'consume', new_callable=AsyncMock)
    yield


@fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
