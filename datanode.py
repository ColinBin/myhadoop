import socket
from config import *
from tools import *
import queue
import threading
from app_route import *
from utilities import *
from functools import reduce

datanode_id_self = None
datanodes_address = None
local_dir = None
map_merged_dir = None
map_merged_self_dir = None
map_merged_final_dir = None
reduce_output_datanode_dir = None

map_task_queue = queue.Queue(maxsize=0)         # map task queue
map_feedback_queue = queue.Queue(maxsize=0)     # feedback from map tasks


def datanode_start():
    """Entrance function for datanode
    
    :return: 
    
    """
    log("START", "namenode started")
    global datanode_id_self, datanodes_address, local_dir, reduce_output_datanode_dir, map_merged_dir

    # to simulate hdfs datanodes should have the same file system structure as the namenode
    output_dir = fs_config['output_dir']
    check_and_make_directory(output_dir)

    datanode_dir = fs_config['datanode_dir']
    check_and_make_directory(datanode_dir)

    # connect namenode
    namenode_ip = net_config['namenode_ip']
    namenode_port = net_config['namenode_port_in']
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((namenode_ip, namenode_port))

    # get file server port and start file server for shuffle
    file_server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    file_server_sock.bind(('0.0.0.0', 0))
    file_server_port = file_server_sock.getsockname()[1]

    file_server_thread = threading.Thread(target=file_server, args=(file_server_sock,))
    file_server_thread.start()

    # send file server port info
    file_server_port_info = {'type': "FILE_SERVER_PORT", "file_server_port": file_server_port}
    send_json_check_echo(sock, file_server_port_info)

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
            job_input_dir = task_info['input_dir']
            job_output_dir = task_info['output_dir']
            log("JOB", "job started --> " + job_name)

            # make temporary directory and reduce output directory for self
            # if directories exist, remove and make
            local_dir = os.path.join(datanode_dir, str(datanode_id_self))
            check_and_make_directory(local_dir)
            log("FS", "temporary directory ready for " + job_name)

            # directory for storing merged map output,
            map_merged_dir = os.path.join(local_dir, "map_merged")
            check_and_make_directory(map_merged_dir)

            reduce_output_datanode_dir = os.path.join(output_dir, str(datanode_id_self))
            check_and_make_directory(reduce_output_datanode_dir)
            log("FS", "output directory ready for " + job_name)

            # call do_the_job
            do_the_job(sock, job_name, job_input_dir, job_output_dir)


def do_the_job(sock, job_name, input_dir, output_dir):
    """Deal with each job
    
    :param sock:
    :param job_name: 
    :param input_dir: 
    :param output_dir: 
    :return: 
    
    """
    global local_dir, datanode_id_self, datanodes_address, map_task_queue, map_feedback_queue, map_merged_dir, map_merged_self_dir, reduce_output_datanode_dir, map_merged_final_dir

    # keep track of map tasks locally
    map_task_local_tracker = dict()

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
            map_task_local_tracker[map_task_id] = "START"

        elif task_type == "MAP_TASK_ASSIGNMENT_END":
            break

    # send back map task feedback
    while True:

        map_feedback_info = map_feedback_queue.get()
        map_task_id = map_feedback_info['map_task_id']

        if map_feedback_info['type'] == "MAP_TASK_DONE":
            # send feedback and update map task progress
            send_json_check_echo(sock, map_feedback_info)
            map_task_local_tracker[map_task_id] = "FINISH"
        elif map_feedback_info['type'] == 'MAP_PARTITION_INFO':
            # send partition info
            send_json_check_echo(sock, map_feedback_info)

        # TODO may send feedback information (partial information) before the task is done

        # check if all map tasks have been done
        # TODO use counters to improve performance
        for status in map_task_local_tracker.values():
            if status != "FINISH":
                # get next feedback
                break
        else:
            # merge map output,
            log("JOB", "map tasks done, start merging files")
            map_merged_self_dir = os.path.join(map_merged_dir, str(datanode_id_self))

            merge_map_output(local_dir, map_merged_dir, map_merged_self_dir, partition_number)
            log("JOB", "finish merging map results")

            # send all-done info and break while loop
            map_datanode_done_info = {'type': "MAP_DATANODE_DONE", "datanode_id": datanode_id_self}
            send_json(sock, map_datanode_done_info)
            break

    # receive shuffle and reduce task
    shuffle_and_reduce_task_info = get_json_echo(sock)
    task_type = shuffle_and_reduce_task_info['type']
    if task_type == 'SHUFFLE_AND_REDUCE':
        shuffle_task_list = shuffle_and_reduce_task_info['shuffle_tasks']
        reduce_task_list = shuffle_and_reduce_task_info['reduce_tasks']

        # shuffle thread list
        shuffle_thread_list = []

        # create socket with each datanode other than self
        for datanode_id, addr_info in datanodes_address.items():
            if int(datanode_id) != datanode_id_self:
                # start shuffle task fetching files from each datanode
                target_datanode_ip = addr_info['ip']
                target_datanode_file_server_port = addr_info['file_server_port']
                # get file in the reduce list for current datanode
                shuffle_thread = threading.Thread(target=thread_shuffle_task, args=(datanode_id, target_datanode_ip, target_datanode_file_server_port, reduce_task_list))
                shuffle_thread_list.append(shuffle_thread)

        for shuffle_thread in shuffle_thread_list:
            shuffle_thread.start()
        for shuffle_thread in shuffle_thread_list:
            shuffle_thread.join()

        log("JOB", "shuffling done")

        # final merge
        map_merged_final_dir = os.path.join(map_merged_dir, "final")
        check_and_make_directory(map_merged_final_dir)
        merge_map_output_final(map_merged_dir, map_merged_final_dir, reduce_task_list, len(datanodes_address))
        log("JOB", "final merge done")

        # final reduce
        app = app_route_info[job_name]()
        reduce_fun = app.reduce
        final_reduce(map_merged_final_dir, reduce_output_datanode_dir, reduce_fun, reduce_task_list)

        log("JOB", "final reduce done")


def thread_shuffle_task(target_datanode_id, target_datanode_ip, file_server_port, shuffle_task_list):
    """Create socket and fetch files from datanode
    
    :return: 
    
    """
    global map_merged_dir, datanodes_address, datanode_id_self
    file_client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    file_client_sock.connect((target_datanode_ip, file_server_port))
    # make directory for storing map result from datanode
    local_dir_datanode = os.path.join(map_merged_dir, str(target_datanode_id))
    check_and_make_directory(local_dir_datanode)

    for target_partition_id in shuffle_task_list:
        file_request_info = {'type': "FILE_REQUEST", 'partition_id': target_partition_id}
        send_json(file_client_sock, file_request_info)

        # get file size info
        file_size_info = get_json_echo(file_client_sock)
        if file_size_info['type'] == "FILE_SIZE":
            file_size = file_size_info['file_size']
            target_file_path = os.path.join(local_dir_datanode, str(target_partition_id))
            get_file(file_client_sock, target_file_path, file_size)

    file_request_over_info = {'type': "FILE_REQUEST_OVER"}
    send_json(file_client_sock, file_request_over_info)
    file_client_sock.close()


def file_server(file_server_sock):
    """File server for shuffle
    
    :param file_server_sock
    :return:
     
    """
    file_server_sock.listen()
    while True:
        sock, addr = file_server_sock.accept()
        serve_file_thread = threading.Thread(target=thread_serve_file, args=(sock,))
        serve_file_thread.start()


def thread_serve_file(sock):
    """Thread serving files through socket
    
    :param sock: 
    :return: 
    
    """
    global map_merged_dir
    while True:
        file_request_info = get_json(sock)
        request_type = file_request_info['type']
        if request_type == "FILE_REQUEST":
            target_partition_id = file_request_info['partition_id']
            target_partition_file_path = os.path.join(map_merged_self_dir, str(target_partition_id))

            # send file size information
            file_size_info = {"type": "FILE_SIZE", "file_size": os.stat(target_partition_file_path).st_size}
            send_json_check_echo(sock, file_size_info)

            send_file(sock, target_partition_file_path)
        elif request_type == "FILE_REQUEST_OVER":
            sock.close()
            break


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

        # send partition info for scheduling
        map_partition_info = {'type': "MAP_PARTITION_INFO", "map_task_id": map_task_id, "datanode_id": datanode_id_self,
                              'partition_info': partition_info}
        map_feedback_queue.put(map_partition_info)

        # send task done info
        map_task_done_info = {"type": "MAP_TASK_DONE", "job_name": job_name, "map_task_id": map_task_id,
                              "status": "FINISH", "datanode_id": datanode_id_self}
        map_feedback_queue.put(map_task_done_info)


def reduce():
    pass


if __name__ == "__main__":
    datanode_start()
