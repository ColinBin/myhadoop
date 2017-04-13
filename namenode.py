import socket
from check import check_dir
from config import *
import threading
from tools import *
import queue
import json

datanode_number = general_config['datanode_number']

input_file_generator_lock = threading.Lock()
datanode_keeper_lock = threading.Lock()

job_queue = queue.Queue(maxsize=0)      # job queue
task_queues = None                      # queues of map or reduce tasks for datanodes
feedback_queue = queue.Queue(maxsize=0) # feedback queue for clients

datanode_address_keeper = dict()        # relate datanode id with ip and port


def namenode_start():
    """Entrance function for namenode
    
    :return: 
    
    """
    global task_queues, datanode_number
    # start job tracker thread
    threading.Thread(target=thread_jobtracker).start()

    # create task queues
    task_queues = [queue.Queue(maxsize=0) for number in list(range(datanode_number))]

    # bind and listen inside port
    namenode_port_in = net_config['namenode_port_in']
    server_sock_in = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_in.bind(('localhost', namenode_port_in))
    server_sock_in.listen()

    # connect with datanodes, start datanode tracker threads
    datanode_tracker_threads = []
    for number in list(range(datanode_number)):
        sock, addr = server_sock_in.accept()
        datanode_address_keeper[number] = addr
        datanode_tracker_thread = threading.Thread(target=thread_datanode_tracker, args=(sock, addr, number))
        datanode_tracker_threads.append(datanode_tracker_thread)
    server_sock_in.close()
    for datanode_thread in datanode_tracker_threads:
        datanode_thread.start()
    # for datanode_thread in datanode_tracker_threads:
    #     datanode_thread.join()

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


def thread_jobtracker():
    """Fetch jobs and track the progress
    
    :return:
     
    """
    log("JOBTRACKER", "jobtracker started")

    global job_queue, task_queues

    while True:
        # get a new job
        job_info_json = job_queue.get()
        job_name = job_info_json['job_name']
        input_dir = job_info_json['input_dir']
        output_dir = job_info_json['output_dir']
        input_file_list = job_info_json['input_file_list']
        log("JOBTRACKER", "new job started --> " + job_name)

        # notifying datanodes about new job
        for datanode_id in list(range(datanode_number)):
            job_info = {"type": "NEW_JOB", "job_name": job_name, "input_dir": input_dir, "output_dir": output_dir}
            task_queues[datanode_id].put(job_info)
        # for file in input_file_list:
        #     for datanode_id in list(range(datanode_number)):
        #         task_queues[datanode_id].put({})


def thread_datanode_tracker(sock, addr, id):
    """Assign tasks, exchange information with datanodes via socket
    
    :param sock: 
    :param addr: 
    :return:
     
    """
    global task_queues, feedback_queue
    while True:
        task_info = task_queues[id].get()
        if task_info['type'] == "NEW_JOB":
            job_name = task_info['job_name']
            input_dir = task_info['input_dir']
            output_dir = task_info['output_dir']

            message = sock.recv(1024).decode('utf-8')
            response = message + job_name
            sock.send(response.encode())

            feedback_info = {"status": "SUCCESS", "message": "job done"}
            feedback_queue.put(feedback_info)


def thread_client(sock, addr):
    """Obtain jobs to run
    
    :param sock: 
    :param addr: 
    :return: 
    
    """
    job_info = get_json(sock)
    # if a new job is submitted, make a record in the queue
    if job_info['request_type'] == "NEW_JOB":
        job_name = job_info['job_name']
        job_fs_path = job_info['job_fs_path']
        input_dir, output_dir, input_file_list = check_dir(job_fs_path)            # check dir for the job
        if len(input_file_list) <= 0:
            feedback_info = {"status": "ERROR", "message": "checking directories failed"}
            send_json(sock, feedback_info)
            return
        feedback_info = {"status": "INFO", "message": "checking directories succeeded"}
        send_json(sock, feedback_info)
        job_queue.put({"job_name": job_name, "input_dir": input_dir,"output_dir": output_dir, "input_file_list": input_file_list})

        # keep sending feedback until SUCCESS
        while True:
            feedback_info = feedback_queue.get()
            send_json(sock, feedback_info)
            if feedback_info['status'] == "SUCCESS":
                break

if __name__ == "__main__":
    namenode_start()
