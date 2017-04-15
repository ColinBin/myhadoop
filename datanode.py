import socket
from config import *
from tools import *
import os
import shutil
import queue
import threading
import json
from app_route import *
from utilities import *
from functools import reduce

datanode_id_self = None
datanodes_address = None
local_dir = None
reduce_output_dir = None

map_task_queue = queue.Queue(maxsize=0)         # map task queue
map_feedback_queue = queue.Queue(maxsize=0)     # feedback from map tasks


def datanode_start():
    """Entrance function for datanode
    
    :return: 
    
    """
    log("START", "namenode started")
    global datanode_id_self, datanodes_address, local_dir, reduce_output_dir
    # connect namenode
    namenode_ip = net_config['namenode_ip']
    namenode_port = net_config['namenode_port_in']
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((namenode_ip, namenode_port))

    # get datanodes address information
    datanodes_ad_info = get_json_echo(sock)
    if datanodes_ad_info['type'] == "DATANODES_AD":
        datanode_id_self = datanodes_ad_info['id_self']
        datanodes_address = datanodes_ad_info['content']

    # start map task thread
    map_thread = threading.Thread(target=thread_map_task)
    map_thread.start()

    # waiting for jobs
    while True:
        task_info = get_json_echo(sock)

        # if there is a new job
        if task_info['type'] == "NEW_JOB":
            job_name = task_info['job_name']
            input_dir = task_info['input_dir']
            output_dir = task_info['output_dir']
            log("JOB", "job started --> " + job_name)

            # make temporary directory and reduce output directory for self
            # if directories exist, remove and make
            datanode_dir = fs_config['datanode_dir']
            local_dir = os.path.join(datanode_dir, str(datanode_id_self))
            check_and_make_directory(local_dir)
            log("FS", "temporary directory ready for " + job_name)
            reduce_output_dir = os.path.join(output_dir, str(datanode_id_self))
            check_and_make_directory(reduce_output_dir)
            log("FS", "output directory ready for " + job_name)

            # call do_the_job
            do_the_job(sock, job_name, input_dir, output_dir)


def do_the_job(sock, job_name, input_dir, output_dir):
    """Deal with each job
    
    :param sock:
    :param job_name: 
    :param input_dir: 
    :param output_dir: 
    :return: 
    
    """
    global local_dir, datanode_id_self, datanodes_address, map_task_queue, map_feedback_queue

    # receive map tasks
    while True:
        # get task info and send echo
        task_info = get_json_echo(sock)

        # if new task, add to the queue
        task_type = task_info['type']
        if task_type == "MAP_TASK":
            map_task_id = task_info['map_task_id']
            map_task_file = task_info['map_task_file']
            map_task = {"job_name": job_name, "input_dir": input_dir, "map_task_id": map_task_id, "map_task_file": map_task_file}
            map_task_queue.put(map_task)

        elif task_type == "MAP_TASK_END":
            break

    # send back map task feedback


def thread_map_task():
    """Fetch map tasks from the queue and execute, send feedback to feedback queue
    
    :return: 
    
    """
    global map_task_queue, map_feedback_queue, datanode_id_self, local_dir

    while True:
        map_task = map_task_queue.get()
        job_name = map_task['job_name']
        input_dir = map_task['input_dir']
        map_task_file = map_task['map_task_file']
        map_task_id = map_task['map_task_id']

        # find app map function
        app = app_route_info[job_name]()
        map_fun = app.map

        # make directory for current map task
        map_task_dir = os.path.join(local_dir, map_task_id)
        check_and_make_directory(map_task_dir)

        # read file and apply map function
        with open(os.path.join(input_dir, map_task_file), 'r', encoding='utf-8') as f:
            content = []
            for line in f:
                for word in line.split():
                    content.append(word)
            raw_map_result = map(map_fun, content)
            sorted_map_result = sorted(raw_map_result, key=get_key_for_sort_partition)

        # write partitions locally and get partition information
        partition_info = partition_sorted(sorted_map_result, map_task_dir)
        print(partition_info)


def shuffle():

    pass


def reduce():
    pass


if __name__ == "__main__":
    datanode_start()
