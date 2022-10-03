import asyncio
import json
import logging
import os

from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage
from dotenv import load_dotenv

from RabbitMQ.database.commandclass import DBCommand, OperationCommand, ShiftCommand, ValidationError

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


async def on_message(message: AbstractIncomingMessage) -> None:
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible. Validates the data
    with pydantic, then tries to store them in database.
    """
    received_message = json.loads(message.body)

    for item in received_message:
        if item['ID_CMD'] == 10:
            try:
                operation: OperationCommand = OperationCommand(
                    command_id=item['ID_CMD'],
                    unit_id=item['ID_UNIT'],
                    result=item['Result'],
                    mode=item['Mode'],
                    datetime=item['DateTime']
                )
                insert_into_table_command: DBCommand = DBCommand(operation)
                await insert_into_table_command.insertIntoTable()
            except ValidationError as e:
                logger.error(e)

        elif item['ID_CMD'] == 30:
            try:
                shift: ShiftCommand = ShiftCommand(
                    command_id=item['ID_CMD'],
                    shift_num=item['SHIFT_NUM'],
                    datetime=item['DateTime']
                )
                insert_into_table_command: DBCommand = DBCommand(shift)
                await insert_into_table_command.insertIntoTable()
            except ValidationError as e:
                logger.error(e)
        else:
            logger.error("No command id was given!")


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
