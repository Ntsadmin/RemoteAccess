import asyncio
import json
import os

from aio_pika import Message, connect


"""
Constants to try if script works correctly
"""
RESULT: list = [{'ID_CMD': 10, 'ID_UNIT': 13, 'Result': 4, 'Mode': 1, 'DateTime': '20.05.2022T16:35:32'}]
SHIFT: list = [{'ID_CMD': 30, 'SHIFT_NUM': 2, 'DateTime': '20.10.2022T16:35:32'}]


async def sendMessage(sentMessage) -> None:
    """
    Simulates a sending process to a consumer. Made to debug main consumer.
    :param sentMessage: list of data to send
    :return:
    """
    connection = await connect(f"amqp://{os.getenv('LOCAL_RABBIT_USER')}:{os.getenv('LOCAL_RABBIT_PASSWORD')}@192.100.1.108:16555/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()
        # Declaring queue
        queue = await channel.declare_queue("new")

        send_message = json.dumps(sentMessage)
        send_message = send_message.encode('utf-8')

        await channel.default_exchange.publish(
            Message(send_message),
            routing_key=queue.name,
        )
        print(f" [x] Sent {sentMessage}'")


if __name__ == "__main__":
    asyncio.run(sendMessage(SHIFT))
