from typing import Optional, AnyStr

import aio_pika


class Connection:
    def __init__(self, user: AnyStr, password: AnyStr, host: AnyStr, virtual_host: Optional[AnyStr] = None,
                 reconnect_interval: int = 5):
        self.user = user
        self.password = password
        self.host = host
        self.virtual_host = virtual_host
        self.connection = None
        self.reconnect_interval = reconnect_interval

    async def connect(self):
        connection = f"amqp://{self.user}:{self.password}@{self.host}"

        if self.virtual_host:
            connection += f"/{self.virtual_host}"

        self.connection = await aio_pika.connect_robust(connection, reconnect_interval=self.reconnect_interval)

    async def channel(self):
        return await self.connection.channel()

    async def close(self):
        await self.connection.close()
