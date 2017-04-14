import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.connect(('localhost', 8888))

to_send = b"Hello World"

s.send(to_send)
s.sendall()

response = s.recv(1024)

print(response)

s.close()