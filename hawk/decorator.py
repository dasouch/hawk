import ujson


def parse_message(func):
    async def inner(message):
        async with message.process():
            message = ujson.loads(message.body)
        await func(message)
    return inner
