"""
main server handler
"""
import asyncio

from RabbitMQ.engines.receiver import RabbitMQ_server


async def main() -> None:
    """
    runs rabbitmq server
    :return:
    """
    await RabbitMQ_server()


if __name__ == '__main__':
    asyncio.run(main())
