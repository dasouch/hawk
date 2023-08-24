from typing import Optional, AnyStr

import aio_pika


class Connection:
    def __init__(self, user: AnyStr, password: AnyStr, host: AnyStr, virtual_host: Optional[AnyStr] = None):
        self.user = user
        self.password = password
        self.host = host
        self.virtual_host = virtual_host
        self.connection = None

    async def connect(self):
        if self.virtual_host:
            self.connection = await aio_pika.connect_robust(
                f"amqp://{self.user}:{self.password}@{self.host}/{self.virtual_host}")
        else:
            self.connection = await aio_pika.connect_robust(
                f"amqp://{self.user}:{self.password}@{self.host}")

    async def channel(self):
        return await self.connection.channel()

    async def close(self):
        await self.connection.close()
