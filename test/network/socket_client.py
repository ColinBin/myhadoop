import socket
from robust_socket_io import RSockIO

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.connect(("localhost", 1111))

to_send = {"name": "Jack", "age": 22}

rsIO = RSockIO(s)

rsIO.sendline(to_send)

response = rsIO.readlineb()[1]

print(response)

rsIO.close_sock()