import asyncio
import aiofiles
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

host = 'localhost'
port = 8080
FORMAT = 'utf-8'


# async def server():
#     print("[SERVER LISTENING]")
#     loop = asyncio.get_event_loop()
#     while True:
#
#         conn, addr = await loop.sock_accept(s)
#         print(f"[CONNECTION ESTABLISHED] {addr}")
#         loop.create_task(handler(conn, loop))

async def server(hoster, ports):
    serv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    serv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serv_socket.setblocking(False)
    serv_socket.bind((hoster, ports))
    serv_socket.listen(10)

    serv = await asyncio.start_server(handler_client, sock=serv_socket, start_serving=False)
    print('[SERVER READY!]')

    async with serv:
        await serv.serve_forever()


async def handler_client(reader, writer):
    request = None
    while request != 'quit':
        request = (await reader.read(255)).decode('utf8')

        response = str(eval(request))
        received_message = []

        writer.write(response.encode('utf8'))
        await writer.drain()

        try:
            first_parentheses, end_parentheses = 0, 0

            for index, item in enumerate(response):
                if item == '{':
                    first_parentheses = index
                if item == '}':
                    end_parentheses = index
                    element = json.dumps(response[first_parentheses:end_parentheses + 1])
                    received_message.append(element)

            async with aiofiles.open('data.txt', 'a') as f:
                message = json.dumps(received_message)
                await f.write(message)

        except json.JSONDecodeError:
            logger.error("Couldn't decode this message !")

    writer.close()


# async def handler(conn, loop):
#     while True:
#         msg = await loop.sock_recv(conn, 1048)
#         print(f"[Socket received:] {msg}")
#         received_message = []
#
#         try:
#             json_dumper = msg.decode(FORMAT)
#
#             first_parentheses = 0
#             end_parentheses = 0
#
#             for i in range(len(json_dumper)):
#                 if json_dumper[i] == '{':
#                     first_parentheses = i
#
#                 if json_dumper[i] == '}':
#                     end_parentheses = i
#                     element = json_dumper[first_parentheses: end_parentheses + 1]
#
#                     element = json.loads(element)
#                     received_message.append(element)
#
#             if len(received_message) == 1:
#                 json_loader = received_message[0]
#
#                 insert_into_table_command = Command(json_loader)
#                 await insert_into_table_command.insertIntoTable()
#
#             else:
#                 for item in received_message:
#                     insert_into_table_command = Command(item)
#                     await insert_into_table_command.insertIntoTable()
#
#         except json.JSONDecodeError:
#             logger.error("couldn't decode or read the message !")
#
#         logger.info(f"{received_message} is added to database !")
#
#         with open('data.txt', 'a') as f:
#             message = json.dumps(received_message)
#             f.write(message)
#
#         if not msg:
#             break
#         await loop.sock_sendall(conn, msg)
#     conn.close()


async def Rabbit_main():
    try:
        connection = await connect(
            f"amqp//{os.getenv('USERNAME')}:{os.getenv('RABBITPASSWORD')}@{os.getenv('IP')}:{os.getenv('RABBITPORT')}/")
        logger.info("connected to rabbitmq")

        async with connection:
            channel = await connection.channel()

            queue = await channel.declare_queue("factory")

            first_parentheses, end_parentheses = 0, 0
            results = []

            async with aiofiles.open('data.txt', 'r+') as f:
                file_data = await f.read()

                for index, word in enumerate(file_data):
                    if word == '{':
                        first_parentheses = index

                    if word == '}':
                        end_parentheses = index
                        element = json.loads(file_data[first_parentheses:end_parentheses + 1])
                        results.append(element)

                await f.truncate(0)

            file_messages = json.dumps(results)
            file_messages = file_messages.encode(FORMAT)

            await channel.default_exchange.publish(
                Message(file_messages),
                routing_key=queue.name
            )

            logger.info("Messages sent though Rabbitmq!")

    except MessageProcessError as e:
        logger.error(e)


if __name__ == '__main__':
    asyncio.run(server(host, port))
