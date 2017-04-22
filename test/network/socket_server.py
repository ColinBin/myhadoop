import socket
from robust_socket_io import RSockIO

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.bind(('0.0.0.0', 1111))

s.listen()

while True:
    sock, addr = s.accept()
    print(addr)
    rsIO = RSockIO(sock)
    received_bytes = rsIO.readlineb()[1]
    print(received_bytes)
    received_str = received_bytes
    to_response_str = received_str + " COLIN"
    to_response = to_response_str
    rsIO.sendline(to_response)
