import socket
from config import *
import threading
from tools import *
import queue
from utilities import *
from robust_socket_io import *

datanode_number = general_config['datanode_number']

job_queue_tracker = queue.Queue(maxsize=0)      # job queue for job tracker
job_queue_scheduler = queue.Queue(maxsize=0)    # job queue for scheduler

task_queues = None                      # queues of map or reduce tasks for datanodes
datanodes_feedback_queue = queue.Queue(maxsize=0)   # queue for receiving feedback from datanodes
client_feedback_queue = queue.Queue(maxsize=0)      # feedback queue for clients
partition_info_queue = queue.Queue(maxsize=0)       # partition info queue
datanode_address_keeper = dict()        # relate datanode id with ip and port

def namenode_start():
    """Entrance function for namenode
    
    :return: 
    
    """
    global task_queues, datanode_number
    # start job tracker thread
    threading.Thread(target=thread_jobtracker).start()

    # start scheduler thread
    threading.Thread(target=thread_scheduler).start()

    # create task queues
    task_queues = [queue.Queue(maxsize=0) for number in list(range(datanode_number))]

    # clear datanode dir
    datanode_dir = fs_config['datanode_dir']
    check_and_make_directory(datanode_dir)

    # bind and listen inside-port
    namenode_port_in = net_config['namenode_port_in']
    server_sock_in = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_in.bind(('0.0.0.0', namenode_port_in))
    server_sock_in.listen()

    # connect with datanodes, start datanode tracker threads
    datanode_tracker_threads = []
    for number in list(range(datanode_number)):
        sock, addr = server_sock_in.accept()
        datanode_ad_info_per = {"ip": addr[0], "port": addr[1]}

        rsock = RSockIO(sock)
        # record file server port of this datanode
        file_server_port_info = get_json(rsock)
        if file_server_port_info['type'] == "FILE_SERVER_PORT":
            datanode_ad_info_per['file_server_port'] = file_server_port_info['file_server_port']

        datanode_address_keeper[number] = datanode_ad_info_per
        datanode_tracker_thread = threading.Thread(target=thread_datanode_tracker, args=(rsock, addr, number))
        datanode_tracker_threads.append(datanode_tracker_thread)
    server_sock_in.close()
    for datanode_thread in datanode_tracker_threads:
        datanode_thread.start()
    # for datanode_thread in datanode_tracker_threads:
    #     datanode_thread.join()

    # bind and listen outside port, waiting for submitted jobs
    namenode_port_out = net_config['namenode_port_out']
    server_sock_out = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_out.bind(('0.0.0.0', namenode_port_out))
    server_sock_out.listen()
    client_threads = []
    while True:
        sock, addr = server_sock_out.accept()
        rsock = RSockIO(sock)
        client_thread = threading.Thread(target=thread_client, args=(rsock, addr))
        client_threads.append(client_thread)
        client_thread.start()


def thread_scheduler():
    """receive partition info and schedule shuffle and reduce 
    
    :return: 
    
    """
    global datanode_number, partition_info_queue, job_queue_scheduler
    log("SCHEDULER", "scheduler stared")
    # wait for new job
    while True:
        job_info = job_queue_scheduler.get()
        job_name = job_info['job_name']

        # keep track of partition info
        partition_info_tracker = dict()
        for datanode_id in list(range(datanode_number)):
            partition_info_tracker[datanode_id] = dict()

        # get partition info from datanodes
        while True:
            partition_info = partition_info_queue.get()
            info_type = partition_info['type']

            # if partition info
            if info_type == "MAP_PARTITION_INFO":
                # merge partition info
                map_task_id = partition_info['map_task_id']
                datanode_id = partition_info['datanode_id']
                partition_info = partition_info['partition_info']
                merge_partition_info(partition_info_tracker[datanode_id], partition_info)
            elif info_type == 'MAP_ALL_DONE':
                # if map all done, break
                log("SCHEDULER", "finish gathering partition info for job " + job_name)
                break
        # print(partition_info_tracker)
        # schedule based on partition info
        schedule(partition_info_tracker)


def schedule(partition_info_tracker):
    """Based on partition info, generate reduce queue and shuffle queue for each datanode
    
    :param partition_info_tracker: 
    :return: 
    
    """
    global datanode_number, task_queues

    # schedule plan to return
    shuffle_task_lists = dict()
    reduce_task_lists = dict()

    # initialize
    for datanode_id in list(range(datanode_number)):
        shuffle_task_lists[datanode_id] = []
        reduce_task_lists[datanode_id] = []

    # locality matrices of size datanode_number * partition_number
    internal_locality = [[0 for partition_id in list(range(partition_number))] for datanode_id in list(range(datanode_number))]
    node_locality = [[0 for partition_id in list(range(partition_number))] for datanode_id in list(range(datanode_number))]
    combined_locality = [[0 for partition_id in list(range(partition_number))] for datanode_id in list(range(datanode_number))]

    # calculate internal locality
    for datanode_id in list(range(datanode_number)):
        partition_info = partition_info_tracker[datanode_id]
        total_workload_datanode = sum(partition_info.values())
        for partition_id in list(range(partition_number)):
            internal_locality[datanode_id][partition_id] = partition_info[partition_id] / total_workload_datanode

    # calculate node locality
    for partition_id in list(range(partition_number)):
        total_workload_partition = sum(partition_info_tracker[datanode_id][partition_id] for datanode_id in list(range(datanode_number)))
        for datanode_id in list(range(datanode_number)):
            node_locality[datanode_id][partition_id] = partition_info_tracker[datanode_id][partition_id] / total_workload_partition

    # calculate combined locality
    for datanode_id in list(range(datanode_number)):
        for partition_id in list(range(partition_number)):
            combined_locality[datanode_id][partition_id] = internal_locality[datanode_id][partition_id] * node_locality[datanode_id][partition_id]

    # calculate average load
    total_workload = sum(sum(partition_info_tracker[datanode_id].values()) for datanode_id in list(range(datanode_number)))
    average_workload = total_workload / datanode_number

    # TODO use heap to manage
    # previous solution
    reduce_decisions = dict()       # partition_id : namenode_id
    datanode_load = dict()          # datanode_id : workload

    # initialize
    for datanode_id in list(range(datanode_number)):
        datanode_load[datanode_id] = 0

    while True:
        # get datanode with minimum workload
        current_datanode_id = 0
        for datanode_id in list(range(datanode_number)):
            if datanode_load[datanode_id] < datanode_load[current_datanode_id]:
                current_datanode_id = datanode_id

        # get maximum locality on current datanode
        datanode_locality_info = combined_locality[current_datanode_id]
        # find first partition not assigned
        current_partition_id = -1
        for partition_id in list(range(partition_number)):
            if partition_id not in reduce_decisions.keys():
                current_partition_id = partition_id
        # all partition assigned
        if current_partition_id == -1:
            break
        for partition_id in list(range(partition_number)):
            if datanode_locality_info[partition_id] > datanode_locality_info[current_partition_id]:
                if partition_id not in reduce_decisions.keys():
                    current_partition_id = partition_id
        reduce_decisions[current_partition_id] = current_datanode_id
        datanode_load[current_datanode_id] = datanode_load[current_datanode_id] + sum(partition_info_tracker[datanode_id][current_partition_id] for datanode_id in list(range(datanode_number)))

    # assign reduce and shuffle tasks
    for partition_id in list(range(partition_number)):
        target_datanode_id = reduce_decisions[partition_id]
        for datanode_id in list(range(datanode_number)):
            if datanode_id == target_datanode_id:
                reduce_task_lists[datanode_id].append(partition_id)
            else:
                shuffle_task_lists[datanode_id].append(partition_id)

    for datanode_id in list(range(datanode_number)):
        reduce_tasks_datanode = reduce_task_lists[datanode_id]
        shuffle_tasks_datanode = shuffle_task_lists[datanode_id]

        # send to the task queues
        shuffle_and_reduce_task_info = {"type": "SHUFFLE_AND_REDUCE", "reduce_tasks": reduce_tasks_datanode, "shuffle_tasks": shuffle_tasks_datanode}
        task_queues[datanode_id].put(shuffle_and_reduce_task_info)

    # print(reduce_decisions)


def thread_jobtracker():
    """Fetch jobs and track the progress
    
    :return:
     
    """
    log("JOBTRACKER", "jobtracker started")

    global job_queue_tracker, task_queues, datanodes_feedback_queue, partition_info_queue

    while True:
        # get a new job
        job_info_json = job_queue_tracker.get()

        # notify scheduler
        job_queue_scheduler.put(job_info_json)

        # get specific information
        job_name = job_info_json['job_name']
        input_dir = job_info_json['input_dir']
        output_dir = job_info_json['output_dir']
        input_file_list = job_info_json['input_file_list']
        log("JOBTRACKER", "new job started --> " + job_name)

        # keep track of datanode progress locally
        datanode_progress_info = dict()

        # notifying datanodes about new job
        for datanode_id in list(range(datanode_number)):
            job_info = {"type": "NEW_JOB", "job_name": job_name, "input_dir": input_dir, "output_dir": output_dir}
            task_queues[datanode_id].put(job_info)
            datanode_progress_info[datanode_id] = "START"

        # assign map tasks to datanodes with file information
        current_datanode_id = 0
        for file in input_file_list:
            task_info = {"type": "MAP_TASK", "map_task_id": job_name + "_m_" + file, "map_task_file": file}
            task_queues[current_datanode_id].put(task_info)
            current_datanode_id = (current_datanode_id + 1) % datanode_number

        # notify ending of map tasks assignment
        for datanode_id in list(range(datanode_number)):
            map_task_assignment_end_info = {"type": "MAP_TASK_ASSIGNMENT_END"}
            task_queues[datanode_id].put(map_task_assignment_end_info)
            datanode_progress_info[datanode_id] = "MAP_TASK_ASSIGNMENT_END"

        # receive map task feedback information
        while True:
            map_feedback_info = datanodes_feedback_queue.get()
            if map_feedback_info['type'] == 'MAP_TASK_DONE':
                task_status = map_feedback_info['status']
                map_task_id = map_feedback_info['map_task_id']
                if task_status == "FINISH":
                    print("map task finished " + map_task_id)
            elif map_feedback_info['type'] == 'MAP_DATANODE_DONE':
                datanode_id = map_feedback_info['datanode_id']
                datanode_progress_info[datanode_id] = "MAP_DATANODE_DONE"
                for status in datanode_progress_info.values():
                    if status != "MAP_DATANODE_DONE":
                        # some datanode has not finished map tasks
                        break
                else:
                    # all map tasks are done, notify scheduler and break
                    map_all_done_scheduler_info = {'type': "MAP_ALL_DONE"}
                    partition_info_queue.put(map_all_done_scheduler_info)
                    break

        # wait for shuffle and reduce feedback


def thread_datanode_tracker(rsock, addr, id):
    """Assign tasks, exchange information with datanodes via socket
    
    :param rsock: 
    :param addr: 
    :return:
     
    """
    global task_queues, client_feedback_queue, datanode_address_keeper, datanodes_feedback_queue

    # send datanode address information
    datanode_ad_info = {"type": "DATANODES_AD", "content": datanode_address_keeper, "id_self": id}
    send_json(rsock, datanode_ad_info)

    while True:
        task_info = task_queues[id].get()
        task_type = task_info['type']

        # if there is a new job
        map_tasks_id_list = []
        if task_type == "NEW_JOB":
            # send and check echo
            send_json(rsock, task_info)

            # for the current job, send map tasks to this datanodes
            while True:
                task_info = task_queues[id].get()
                task_type = task_info['type']

                # keep track of map tasks locally
                if task_type == "MAP_TASK":
                    map_tasks_id_list.append(task_info['map_task_id'])

                # send map task info and check echo
                send_json(rsock, task_info)

                # if no more map tasks, break
                if task_type == "MAP_TASK_ASSIGNMENT_END":
                    break

            # waiting for map tasks feedback
            while True:
                map_feedback_info = get_json(rsock)
                feedback_type = map_feedback_info['type']
                if feedback_type == "MAP_TASK_DONE":
                    datanodes_feedback_queue.put(map_feedback_info)
                elif feedback_type == "MAP_PARTITION_INFO":
                    # for map partition info, add to partition info queue
                    partition_info_queue.put(map_feedback_info)
                elif feedback_type == "MAP_DATANODE_DONE":
                    # if assigned map tasks all done, break
                    datanodes_feedback_queue.put(map_feedback_info)
                    break

            # receive shuffle and reduce task
            shuffle_and_reduce_task_info = task_queues[id].get()
            task_type = shuffle_and_reduce_task_info['type']
            if task_type == "SHUFFLE_AND_REDUCE":
                send_json(rsock, shuffle_and_reduce_task_info)

            # waiting for shuffle and reduce feedback


def thread_client(rsock, addr):
    """Obtain jobs to run
    
    :param sock: 
    :param addr: 
    :return: 
    
    """
    job_info = get_json(rsock)
    # if a new job is submitted, make a record in the queue
    if job_info['type'] == "NEW_JOB":
        job_name = job_info['job_name']
        job_fs_path = job_info['job_fs_path']
        input_dir, output_dir, input_file_list = check_dir_for_job(job_fs_path)            # check dir for the job
        if len(input_file_list) <= 0:
            feedback_info = {"type": "FEEDBACK", "status": "ERROR", "message": "checking directories failed"}
            send_json(rsock, feedback_info)
            return
        feedback_info = {"type": "FEEDBACK", "status": "INFO", "message": "checking directories succeeded"}
        send_json(rsock, feedback_info)
        job_queue_tracker.put({"job_name": job_name, "input_dir": input_dir,"output_dir": output_dir, "input_file_list": input_file_list})

        # keep sending feedback until SUCCESS
        while True:
            feedback_info = client_feedback_queue.get()
            send_json(rsock, feedback_info)
            if feedback_info['status'] == "SUCCESS":
                break

if __name__ == "__main__":
    namenode_start()
