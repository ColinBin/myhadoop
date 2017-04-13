import socket
from check import check_dir
from config import *
import threading
from tools import *
import queue
import json

scheduler_start_lock = threading.Lock()
input_file_generator_lock = threading.Lock()
datanode_keeper_lock = threading.Lock()
job_queue_lock = threading.Lock()

job_queue = queue.Queue(maxsize=0)     # task queue

input_file_generator = None             # iterator for input files

input_dir = None                        # input dir

datanode_address_keeper = dict()        # relate datanode id with ip and port


def namenode_start():
    """Entrance function for namenode
    
    :return: 
    
    """
    # check dirs and get input file list
    global input_file_generator, input_dir, datanode_address_keeper
    input_dir, input_file_list = check_dir()
    input_file_generator = (file for file in input_file_list)

    # start scheduler thread
    scheduler_start_lock.acquire()
    threading.Thread(target=thread_scheduler).start()

    # bind and listen inside port
    namenode_port_in = net_config['namenode_port_in']
    server_sock_in = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_in.bind(('localhost', namenode_port_in))
    server_sock_in.listen()

    # connect with datanodes
    datanode_threads = []
    datanode_number = general_config['datanode_number']
    for number in list(range(1, datanode_number + 1)):
        sock, addr = server_sock_in.accept()
        datanode_address_keeper[number] = addr
        datanode_thread = threading.Thread(target=thread_datanode, args=(sock, addr))
        datanode_threads.append(datanode_thread)
    server_sock_in.close()

    for datanode_thread in datanode_threads:
        datanode_thread.start()
    for datanode_thread in datanode_threads:
        datanode_thread.join()

    # bind and listen outside port, waiting for submitted jobs
    namenode_port_out = net_config['namenode_port_out']
    server_sock_out = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_out.bind(('localhost', namenode_port_out))
    server_sock_out.listen()
    client_threads = []
    while True:
        sock, addr = server_sock_out.accept()
        client_thread = threading.Thread(target=thread_client, args=(sock, addr))
        client_threads.append(client_thread)
        client_thread.start()


def thread_scheduler():
    """Scheduler Thread for scheduling reduce tasks and shuffling
    
    :return:
     
    """
    scheduler_start_lock.acquire()
    print("Hello")


def thread_datanode(sock, addr):
    received_message = sock.recv(1024)
    response = received_message.decode('utf-8') + "Colin"
    response = response.encode()
    sock.send(response)
    sock.close()
    pass


def thread_client(sock, addr):
    """Received jobs to run
    
    :param sock: 
    :param addr: 
    :return: 
    
    """
    job_info = get_json(sock)
    # if a new job is submitted, make a record in the queue
    if job_info['request_type'] == "NEW_JOB":
        job_name = job_info['job_name']
        job_fs_path = job_info['job_fs_path']
        job_queue_lock.acquire()
        job_queue.put((job_name, job_fs_path))
        job_queue_lock.release()

if __name__ == "__main__":
    namenode_start()
