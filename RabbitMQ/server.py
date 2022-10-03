import asyncio
import aiofiles
import json
import os
import logging
import socket

from aio_pika import connect, Message, MessageProcessError
from dotenv import load_dotenv


logging.basicConfig(filename='server.log', level=logging.INFO, format='%(asctime)s:%(message)s')

# Create the logging 
logging.basicConfig(filename='server.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Loading the .env parameters
load_dotenv()

# Giving the log the depth, that should be monitored
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
    Handles async incoming messages and resends message to client. By taking the message, you can manipulate them even
    if the connection between client and server has been terminated.
    :param reader: reads the incoming messages
    :param writer: sends messages to the client
    :return:
    """
    request = None
    while True:
        request = (await reader.read(255)).decode('utf8')
        client_ip_address, port = reader._transport.get_extra_info('peername')
        if not request:
            break
        logger.info(f'received message {request} from {client_ip_address}')

        writer.write(request.encode('utf8'))
        await writer.drain()
        try:
            received_message: list = []
            first_parentheses: int = 0
            for index, item in enumerate(request):
                if item == '{':
                    first_parentheses = index
                if item == '}':
                    end_parentheses: int = index
                    element = json.loads(request[first_parentheses:end_parentheses + 1])
                    received_message.append(element)

            async with aiofiles.open('data.txt', 'a') as f:
                message = json.dumps(received_message)
                await f.write(message)

            # await Rabbit_main()

        except json.JSONDecodeError:
            logger.error("Couldn't decode this message !")
        writer.close()


async def Rabbit_main():
    """
    Script to send messages thought RabbitMQ. reads and formats the data inside 'data.txt', then sends them to consumer.
    :return:
    """
    try:
        connection = await connect(f"amqp://{os.getenv('RABBIT_USER')}:{os.getenv('RABBIT_PASSWORD')}@192.100.1.108:16555/")
        logger.info("connected to rabbitmq")

        async with connection:
            channel = await connection.channel()

            queue = await channel.declare_queue("new")

            first_parentheses: int = 0
            results = []

            async with aiofiles.open('data.txt', 'r+') as f:
                file_data = await f.read()

                for index, word in enumerate(file_data):
                    if word == '{':
                        first_parentheses = index

                    if word == '}':
                        end_parentheses: int = index
                        element = file_data[first_parentheses: end_parentheses + 1]
                        element = json.loads(element)
                        results.append(element)

                await f.truncate(0)

            file_messages = json.dumps(results)
            file_messages = file_messages.encode(os.getenv('FORMAT'))

            await channel.default_exchange.publish(
                Message(file_messages),
                routing_key=queue.name
            )

            logger.info("Messages sent though Rabbitmq!")

    except MessageProcessError as e:
        logger.error(e)


if __name__ == '__main__':
    asyncio.run(server(os.getenv('LOCALHOST'), int(os.getenv('LOCAL_PORT'))))
