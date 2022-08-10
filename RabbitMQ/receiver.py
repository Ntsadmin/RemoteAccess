import asyncio
import json
import logging
from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage
from commandclass import Command

logging.basicConfig(filename='receiver.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


async def on_message(message: AbstractIncomingMessage) -> None:
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible.
    """
    logger.info(f"{message.body} received!")

    received_message = json.loads(message.body)
    print(received_message)

    # if len(received_message) == 1:
    #     insert_into_table_command = Command(received_message[0])
    #     await insert_into_table_command.insertIntoTable()
    #
    # else:
    #     for item in received_message:
    #         insert_into_table_command = Command(item)
    #         await insert_into_table_command.insertIntoTable()


async def main() -> None:
    # Perform connection
    connection = await connect("amqp://admin:kjiflm@193.41.79.46:16555/")
    async with connection:
        # Creating a channel
        channel = await connection.channel()
        # Declaring queue
        queue = await channel.declare_queue("npfactory")
        # Start listening the queue with name 'hello'
        await queue.consume(on_message, no_ack=True)
        print(" [*] Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
