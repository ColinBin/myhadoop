import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.bind(('localhost', 8888))

s.listen()

while True:
    sock, addr = s.accept()
    print(addr)
    received_str = sock.recv(1024).decode('utf-8')
    to_response_str = received_str + " COLIN"
    to_response = to_response_str.encode()
    sock.send(to_response)

