import socket
from config import *
import json


def datanode_start():
    """Entrance function for datanode
    
    :return: 
    
    """
    # connect namenode
    namenode_ip = net_config['namenode_ip']
    namenode_port = net_config['namenode_port_in']
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((namenode_ip, namenode_port))

    to_send = b'Hello World'
    sock.send(to_send)
    response = sock.recv(1024)
    print(response)
    sock.close()


if __name__ == "__main__":
    datanode_start()
