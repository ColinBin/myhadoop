import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.bind(('localhost', 0))

print(s.getsockname())

s.close()
