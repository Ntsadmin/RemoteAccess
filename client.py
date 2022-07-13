import socket
import json

HEADER = 1048
PORT = 8080
SERVER = '192.168.225.26'
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = 'Disconnected'

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)

json_message = [{'ID_CMD': 30, 'Shiftnum': 2, 'Datetime': '20.05.2022T16:35:32'}]
json_result = json.dumps(json_message, ensure_ascii=False)


def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(message)
    print(client.recv(2048).decode(FORMAT))


def start_client():
    message = input("--> ")
    while message != DISCONNECT_MESSAGE:
        send(message)
        message = input("--> ")

    message = message.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    print(client.recv(2048).decode(FORMAT))


send(json_result)
# send(DISCONNECT_MESSAGE)
# start_client()
