import asyncio
import json
import os
import logging
import socket

from aio_pika import connect, Message, MessageProcessError
from commandclass import Command
from dotenv import load_dotenv

logging.basicConfig(filename='server.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

host = '192.100.1.35'
port = 8080
FORMAT = 'utf-8'

loop = asyncio.get_event_loop()
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.setblocking(False)
s.bind((host, port))
s.listen(10)


async def server():
    while True:
        conn, addr = await loop.sock_accept(s)
        print(f"[CONNECTION ESTABLISHED] {addr}")
        loop.create_task(handler(conn))


async def handler(conn):
    while True:
        msg = await loop.sock_recv(conn, 1048)
        logger.info(f"Socket received: {msg}")
        received_message = []

        try:
            json_dumper = msg.decode(FORMAT)

            first_parentheses = 0
            end_parentheses = 0

            for i in range(len(json_dumper)):
                if json_dumper[i] == '{':
                    first_parentheses = i

                if json_dumper[i] == '}':
                    end_parentheses = i
                    element = json_dumper[first_parentheses: end_parentheses + 1]

                    element = json.loads(element)
                    received_message.append(element)

            if len(received_message) == 1:
                json_loader = received_message[0]

                insert_into_table_command = Command(json_loader)
                await insert_into_table_command.insertIntoTable()

            else:
                for item in received_message:
                    insert_into_table_command = Command(item)
                    await insert_into_table_command.insertIntoTable()

        except json.JSONDecodeError:
            logger.error("couldn't decode or read the message !")

        logger.info(f"{received_message} is added to database !")

        with open('data.txt', 'a') as f:
            message = json.dumps(received_message)
            f.write(message)

        if not msg:
            break
        await loop.sock_sendall(conn, msg)
    conn.close()


async def Rabbit_main():
    try:
        connection = await connect(
            f"amqp//{os.getenv('USERNAME')}:{os.getenv('RABBITPASSWORD')}@{os.getenv('IP')}:{os.getenv('RABBITPORT')}/")
        logger.info("connected to rabbitmq")

        async with connection:
            channel = await connection.channel()

            queue = await channel.declare_queue("factory")

            first_parentheses = 0
            end_parentheses = 0

            results = []

            with open('data.txt', 'r+') as f:
                file_data = f.read()

                for i in range(len(file_data)):
                    if file_data[i] == '{':
                        first_parentheses = i

                    if file_data[i] == '}':
                        end_parentheses = i
                        element = file_data[first_parentheses:end_parentheses + 1]
                        element = json.loads(element)
                        results.append(element)

                f.truncate(0)

            file_messages = json.dumps(results)
            file_messages = file_messages.encode(FORMAT)

            await channel.default_exchange.publish(
                Message(file_messages),
                routing_key=queue.name
            )

            logger.info("Messages sent though Rabbitmq!")

    except MessageProcessError as e:
        logger.error(e)


loop.create_task(server())
loop.run_forever()
loop.close()
