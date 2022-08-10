import asyncio
import logging
import aio_pika
import os
from aio_pika import Message, connect
import json

message = b'{\r\n"ID_CMD":10,\r\n"ID_UNIT":13,\r\n"Result":1,\r\n"Mode":1,\r\n"DateTime":"20.05.2022T16:35:32"\r\n}{' \
          b'\r\n"ID_CMD":10,\r\n"ID_UNIT":12,\r\n"Result":1,\r\n"Mode":1,\r\n"DateTime":"20.05.2022T16:35:32"\r\n} '
result = [{'ID_CMD': 10, 'ID_UNIT': 13, 'Result': 1, 'Mode': 1, 'DateTime': '20.05.2022T16:35:32'}]
json_message = message.decode('utf-8')


async def sendMessage(sentMessage) -> None:
    connection = await connect("amqp://asuadmin:kjiflm@192.100.1.108:16555/")
    print("connected")

    firstParentheses = 0
    endParentheses = 0
    results = []

    async with connection:
        # Creating a channel
        channel = await connection.channel()
        # Declaring queue
        queue = await channel.declare_queue("rpthird")

        send_message = json.dumps(sentMessage)
        send_message = send_message.encode('utf-8')

        await channel.default_exchange.publish(
            Message(send_message),
            routing_key=queue.name,
        )
        # try:
        #     if os.stat('data.txt').st_size != 0:
        #         with open('data.txt', "r+") as f:
        #             file_data = f.read()
        #
        #             for i in range(len(file_data)):
        #
        #                 if file_data[i] == '{':
        #                     firstParentheses = i
        #
        #                 if file_data[i] == '}':
        #                     endParentheses = i
        #                     element = file_data[firstParentheses:endParentheses + 1]
        #                     element = json.loads(element)
        #                     results.append(element)
        #
        #             f.truncate(0)
        #     file_result = json.dumps(results)
        #     file_result = file_result.encode('utf-8')
        #
        #     await channel.default_exchange.publish(
        #         Message(file_result),
        #         routing_key=queue.name,
        #     )
        #     logging.info(result)
        #
        #     send_message = json.dumps(sentMessage)
        #     send_message = send_message.encode('utf-8')
        #     await channel.default_exchange.publish(
        #         Message(send_message),
        #         routing_key=queue.name,
        #     )
        #     logging.info(sentMessage)
        # except aio_pika.MessageProcessError as e:
        #     logging.error(e)

        print(f" [x] Sent {results}'")

if __name__ == "__main__":
    asyncio.run(sendMessage(result))
