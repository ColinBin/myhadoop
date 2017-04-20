import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.connect(("localhost", 8888))

to_send = {"name": "Jack", "age": 22}


s.send(to_send)

response = s.recv(1024)

print(response)

s.close()