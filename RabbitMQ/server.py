import asyncio
import aiofiles
import json
import os
import logging
import socket

from aio_pika import connect, Message, MessageProcessError
from commandclass import Command
from dotenv import load_dotenv

# Create the logging 
logging.basicConfig(filename='server.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Loading the .env parameters 
load_dotenv()

# Giving the log the depth, that should be monitored
logger = logging.getLogger()
logger.setLevel(logging.INFO)

host = 'localhost'
port = 8080
FORMAT = 'utf-8'


async def server(hoster, ports):
    """
    Creating socket for client-server communication
    """
    
    serv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    serv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # Give SSL access to the created socket
    serv_socket.setblocking(False)
    serv_socket.bind((hoster, ports))
    serv_socket.listen(10)

    # Handling the connection with another function
    serv = await asyncio.start_server(handler_client, sock=serv_socket, start_serving=False)
    print('[SERVER READY!]')

    # Keeping the connection running no matter what is the result 
    async with serv:
        await serv.serve_forever()


async def handler_client(reader, writer):
    """
    Getting the response from the client, and sending back a closing request 
    """
    request = None
    while request != 'quit':
        # Getting the response from client
        request = (await reader.read(255)).decode('utf8')

        response = str(eval(request))
        received_message = []

        writer.write(response.encode('utf8'))
        await writer.drain()

        try:
            # Storing the response inside a list 
            first_parentheses, end_parentheses = 0, 0

            for index, item in enumerate(response):
                if item == '{':
                    first_parentheses = index
                if item == '}':
                    end_parentheses = index
                    element = json.dumps(response[first_parentheses:end_parentheses + 1])
                    received_message.append(element)

            # Writing the list inside a file to use it latter in a RabbitMQ 
            async with aiofiles.open('data.txt', 'a') as f:
                message = json.dumps(received_message)
                await f.write(message)

        except json.JSONDecodeError:
            logger.error("Couldn't decode this message !")

    writer.close()


async def Rabbit_main():
    """
    Providing a good connection between 2 computers without fear of getting it interrupted by some errors
    """
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
