import asyncio
import json
import logging
import os

from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage
from dotenv import load_dotenv

from RabbitMQ.commandclass import DBCommand, OperationCommand, ShiftCommand, ValidationError

load_dotenv()

logging.basicConfig(filename='receiver.log', level=logging.INFO, format='%(asctime)s:%(message)s')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


async def on_message(message: AbstractIncomingMessage) -> None:
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible. Validates the data
    with pydantic, then tries to store them in database.
    """
    received_message = json.loads(message.body)
    print(received_message)

    for item in received_message:
        print(item)
        if item['ID_CMD'] == 10:
            try:
                operation: OperationCommand = OperationCommand(
                    command_id=item['ID_CMD'],
                    unit_id=item['ID_UNIT'],
                    result=item['Result'],
                    mode=item['Mode'],
                    datetime=item['DateTime']
                )
                print(operation.unit_id)
                # insert_into_table_command: DBCommand = DBCommand(operation)
                # await insert_into_table_command.insertIntoTable()
            except ValidationError as e:
                logger.error(e)

        elif item['ID_CMD'] == 30:
            try:
                shift: ShiftCommand = ShiftCommand(
                    command_id=item['ID_CMD'],
                    shift_num=item['SHIFT_NUM'],
                    datetime=item['DateTime']
                )
                print(shift.shift_num)
                # insert_into_table_command: DBCommand = DBCommand(shift)
                # await insert_into_table_command.insertIntoTable()
            except ValidationError as e:
                logger.error(e)
        else:
            logger.error("No command id was given!")

    # if len(received_message) == 1:
    #     insert_into_table_command = Command(received_message[0])
    #     await insert_into_table_command.insertIntoTable()
    #
    # else:
    #     for item in received_message:
    #         insert_into_table_command = Command(item)
    #         await insert_into_table_command.insertIntoTable()


async def RabbitMQ_server() -> None:
    """
    Starts a RabbitMQ consumer. Consumes the messages and
     handle them with async function 'on_message'
    """
    # Perform connection
    connection = await connect(f"amqp://{os.getenv('RABBIT_USER')}:{os.getenv('RABBIT_PASSWORD')}"
                               f"@{os.getenv('LOCAL_RABBIT_IP')}:16555/")
    async with connection:
        # Creating a channel
        channel = await connection.channel()
        # Declaring queue
        queue = await channel.declare_queue(os.getenv('RABBIT_QUEUE'))
        # Start listening the queue with name 'hello'
        await queue.consume(on_message, no_ack=True)
        print(" [*] Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()


async def main() -> None:
    """
    starts the script
    """
    await RabbitMQ_server()


if __name__ == "__main__":
    asyncio.run(main())
