import socket
import queue
import threading
from app_route import *
from utilities import *
from robust_socket_io import *
import time

datanode_number = None
datanode_id_self = None
datanodes_address = None
local_dir = None
map_merged_dir = None
map_merged_self_dir = None
map_merged_final_dir = None
reduce_output_datanode_dir = None

map_task_queue = queue.Queue(maxsize=0)             # map task queue
map_feedback_queue = queue.Queue(maxsize=0)         # feedback from map tasks

local_reduce_done_queues = None                           # shuffle queue for thread with each datanode

local_reduce_done_tracker = []
local_reduce_done_lock = threading.Lock()

local_reduce_queue = queue.Queue(maxsize=0)         # queue for local reduce thread
final_reduce_queue = queue.Queue(maxsize=0)         # queue for final reduce reduce tasks

shuffle_out_lr_queue = queue.Queue(maxsize=0)       # shuffle out queue for local reducer

final_reduce_partition_queue = queue.Queue(maxsize=0)

final_reduce_started_queue = queue.Queue(maxsize=0) # queue for local reduce to decide whether the reduce partition is to be local reduced


def datanode_start():
    """Entrance function for datanode
    
    :return: 
    
    """
    log("START", "datanode started")
    global datanode_id_self, datanodes_address, local_dir, reduce_output_datanode_dir, map_merged_dir, datanode_number

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
    rsock = RSockIO(sock)

    # start file server thread. Get the port number and send to namenode
    file_server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    file_server_sock.bind(('0.0.0.0', 0))
    file_server_port = file_server_sock.getsockname()[1]

    file_server_thread = threading.Thread(target=file_server, args=(file_server_sock, ))
    file_server_thread.start()

    # send file server port info
    file_server_port_info = {'type': "FILE_SERVER_PORT", "file_server_port": file_server_port}
    send_json(rsock, file_server_port_info)

    # get datanodes address information
    datanodes_ad_info = get_json(rsock)
    if datanodes_ad_info['type'] == "DATANODES_AD":
        datanode_id_self = datanodes_ad_info['id_self']
        datanodes_address = datanodes_ad_info['content']

    datanode_number = len(datanodes_address)

    # start map task thread
    map_thread = threading.Thread(target=thread_map_task)
    map_thread.start()

    # waiting for jobs
    while True:
        task_info = get_json(rsock)

        # if there is a new job
        if task_info['type'] == "NEW_JOB":
            job_name = task_info['job_name']
            job_input_dir = task_info['input_dir']
            job_output_dir = task_info['output_dir']
            job_schedule_plan = task_info['schedule_plan']
            log("JOB", "job started --> " + job_name)

            # make temporary directory and reduce output directory for self
            # if directories exist, remove and make
            local_dir = os.path.join(datanode_dir, make_datanode_dir_name(datanode_id_self))
            check_and_make_directory(local_dir)
            log("FS", "temporary directory ready for " + job_name)

            # directory for storing merged map output,
            map_merged_dir = os.path.join(local_dir, "map_merged")
            check_and_make_directory(map_merged_dir)

            reduce_output_datanode_dir = os.path.join(output_dir, make_datanode_dir_name(datanode_id_self))
            check_and_make_directory(reduce_output_datanode_dir)
            log("FS", "output directory ready for " + job_name)

            # call do_the_job
            do_the_job(rsock, job_name, job_input_dir, job_output_dir, job_schedule_plan)


def do_the_job(rsock, job_name, input_dir, output_dir, job_schedule_plan):
    """Deal with each job
    
    :param rsock:
    :param job_name: 
    :param input_dir: 
    :param output_dir: 
    :param job_schedule_plan
    :return: 
    
    """
    global local_dir, datanode_id_self, datanodes_address, map_task_queue, map_feedback_queue, map_merged_dir, map_merged_self_dir, reduce_output_datanode_dir, map_merged_final_dir, local_reduce_done_queues, datanode_number
    global local_reduce_done_tracker, shuffle_out_lr_queue

    job_start_time = time.time()

    # initialize shuffle queues for each job
    local_reduce_done_queues = [queue.Queue(maxsize=0) for datanode_id in list(range(datanode_number))]

    # keep track of map tasks locally
    map_task_local_tracker = dict()

    # receive map tasks
    while True:
        # get task info and send echo
        task_info = get_json(rsock)

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
            send_json(rsock, map_feedback_info)
            map_task_local_tracker[map_task_id] = "FINISH"
        elif map_feedback_info['type'] == 'MAP_PARTITION_INFO':
            # send partition info
            send_json(rsock, map_feedback_info)

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
            map_merged_self_dir = os.path.join(map_merged_dir, make_datanode_dir_name(datanode_id_self))

            # send all-done info and break while loop
            map_datanode_done_info = {'type': "MAP_DATANODE_DONE", "datanode_id": datanode_id_self}
            send_json(rsock, map_datanode_done_info)
            break

    job_map_done_time = time.time()

    merge_map_output(local_dir, map_merged_dir, map_merged_self_dir, partition_number)
    log("JOB", "finish merging map results")

    local_merge_done_info = {'type': "LOCAL_MAP_MERGE_DONE", "datanode_id": datanode_id_self}
    send_json(rsock, local_merge_done_info)

    # prepare for final merge
    map_merged_final_dir = os.path.join(map_merged_dir, "final")
    check_and_make_directory(map_merged_final_dir)

    # TODO initialize the trackers
    local_reduce_done_tracker = []

    shuffle_out_lr_queue = queue.Queue(maxsize=0)

    final_reduce_started_queue.put([])

    job_received_plan_time = time.time()

    # receive shuffle and reduce task
    shuffle_and_reduce_task_info = get_json(rsock)
    task_type = shuffle_and_reduce_task_info['type']
    if task_type == 'SHUFFLE_AND_REDUCE':
        shuffle_task_list = shuffle_and_reduce_task_info['shuffle_tasks']
        reduce_task_list = shuffle_and_reduce_task_info['reduce_tasks']

        # final reduce
        app = app_route_info[job_name]()
        reduce_fun = app.reduce

        # send configuration information to final reduce thread
        final_reduce_thread = threading.Thread(target=thread_final_reduce, args=(reduce_fun, map_merged_dir, map_merged_final_dir, reduce_output_datanode_dir, reduce_task_list[:], job_schedule_plan, ))
        final_reduce_thread.start()

        # apply different strategies based on schedule plan
        if job_schedule_plan == "HADOOP":
            # shuffle (file client) thread
            shuffle_thread_list = do_shuffle(datanodes_address, datanode_id_self, reduce_task_list[:], job_schedule_plan)

            # put into shuffle queues for file server
            for partition_id in shuffle_task_list:
                for shuffle_queue in local_reduce_done_queues:
                    shuffle_queue.put(partition_id)

            # shuffle thread start and join
            for shuffle_thread in shuffle_thread_list:
                shuffle_thread.start()
            for shuffle_thread in shuffle_thread_list:
                shuffle_thread.join()

            # send shuffle done
            log("JOB", "shuffling done")
            shuffle_done_info = {'type': 'SHUFFLE_DONE', "job_name": job_name, "datanode_id": datanode_id_self}
            send_json(rsock, shuffle_done_info)

            # start final reduce tasks
            for partition_id in reduce_task_list:
                final_reduce_info = {'type': "FINAL_REDUCE_PARTITION", 'partition_id': partition_id}
                final_reduce_queue.put(final_reduce_info)

            # wait for final reduce to be done
            final_reduce_thread.join()

            # notify final reduce done
            log("JOB", "final reduce done")
            job_done_info = {'type': "FINAL_REDUCE_DONE", "job_name": job_name, "datanode_id": datanode_id_self}
            send_json(rsock, job_done_info)

        elif job_schedule_plan == "ICPP":

            # make shuffle queues ready, so that it does not matter whether some partition have been local reduced
            for datanode_id in list(range(datanode_number)):
                for partition_id in shuffle_task_list:
                    local_reduce_done_queues[datanode_id].put(partition_id)

            awaiting_thread_list = []

            # local reduce
            local_reduce_thread = do_local_reduce(map_merged_self_dir, reduce_fun, job_schedule_plan)
            awaiting_thread_list.append(local_reduce_thread)

            # assign local reduce tasks
            for partition_id in shuffle_task_list:
                local_reduce_info = {'type': "LOCAL_REDUCE_PARTITION", 'partition_id': partition_id, "to_shuffle": True}
                local_reduce_queue.put(local_reduce_info)

            # shuffle
            shuffle_thread_list = do_shuffle(datanodes_address, datanode_id_self, reduce_task_list[:], job_schedule_plan)
            awaiting_thread_list = awaiting_thread_list + shuffle_thread_list
            for awaiting_thread in awaiting_thread_list:
                awaiting_thread.start()
            for awaiting_thread in awaiting_thread_list:
                awaiting_thread.join()

            # send shuffle done
            log("JOB", "shuffling done")
            shuffle_done_info = {'type': 'SHUFFLE_DONE', "job_name": job_name, "datanode_id": datanode_id_self}
            send_json(rsock, shuffle_done_info)

            # local reduce the partitions in the reduce task list
            # assign local reduce tasks
            for partition_id in reduce_task_list:
                local_reduce_info = {'type': "LOCAL_REDUCE_PARTITION", 'partition_id': partition_id, "to_shuffle": False}
                local_reduce_queue.put(local_reduce_info)

            local_reduce_thread = do_local_reduce(map_merged_self_dir, reduce_fun, job_schedule_plan)
            local_reduce_thread.start()
            local_reduce_thread.join()

            # start final reduce tasks
            for partition_id in reduce_task_list:
                final_reduce_info = {'type': "FINAL_REDUCE_PARTITION", 'partition_id': partition_id}
                final_reduce_queue.put(final_reduce_info)

            final_reduce_thread.join()

            # notify final reduce done
            log("JOB", "final reduce done")
            job_done_info = {'type': "FINAL_REDUCE_DONE", "job_name": job_name, "datanode_id": datanode_id_self}
            send_json(rsock, job_done_info)

        elif job_schedule_plan == "NEW":

            # assign local reduce tasks
            for partition_id in shuffle_task_list:
                local_reduce_info = {'type': "LOCAL_REDUCE_PARTITION", 'partition_id': partition_id,
                                     "to_shuffle": True}
                local_reduce_queue.put(local_reduce_info)
            for partition_id in reduce_task_list:
                local_reduce_info = {'type': "LOCAL_REDUCE_PARTITION", 'partition_id': partition_id,
                                     "to_shuffle": False}
                local_reduce_queue.put(local_reduce_info)

            awaiting_thread_list = []
            local_reduce_thread = do_local_reduce(map_merged_self_dir, reduce_fun, job_schedule_plan)
            awaiting_thread_list.append(local_reduce_thread)

            # reverse the reduce task list
            shuffle_thread_list = do_shuffle(datanodes_address, datanode_id_self, reduce_task_list[:], job_schedule_plan)
            awaiting_thread_list = awaiting_thread_list + shuffle_thread_list

            # overlapping thread logic
            for awaiting_thread in awaiting_thread_list:
                awaiting_thread.start()
            for awaiting_thread in awaiting_thread_list:
                awaiting_thread.join()

            # send shuffle done
            log("JOB", "shuffling done")
            shuffle_done_info = {'type': 'SHUFFLE_DONE', "job_name": job_name, "datanode_id": datanode_id_self}
            send_json(rsock, shuffle_done_info)

            final_reduce_thread.join()

            # notify final reduce done
            log("JOB", "final reduce done")
            job_done_info = {'type': "FINAL_REDUCE_DONE", "job_name": job_name, "datanode_id": datanode_id_self}
            send_json(rsock, job_done_info)

        job_done_time = time.time()

        # total time used to finish map tasks
        map_time = get_time_in_ms(job_map_done_time - job_start_time)

        # total time used to receive schedule
        make_schedule_time = get_time_in_ms(job_received_plan_time - job_map_done_time)

        # total time used to carry the schedule plan and finish the job
        exec_schedule_time = get_time_in_ms(job_done_time - job_received_plan_time)

        # total time to finish the job
        job_time = get_time_in_ms(job_done_time - job_start_time)

        # job done
        log("JOB", "job done")
        log_time("map", map_time)
        log_time("merging and waiting", make_schedule_time)
        log_time("executing schedule", exec_schedule_time)
        log_time("datanode job", job_time)

        # send time feedback
        time_info = {"map": map_time, "merge_and_wait": make_schedule_time, "exec_schedule": exec_schedule_time, "datanode_job": job_time}
        time_feedback_info = {'type': "TIME_FEEDBACK", "datanode_id": datanode_id_self, "time_info": time_info}
        send_json(rsock, time_feedback_info)

        # send time feedback done
        time_feedback_info = {'type': "TIME_FEEDBACK_DONE", "datanode_id": datanode_id_self}
        send_json(rsock, time_feedback_info)


def do_shuffle(datanodes_address, datanode_id_self, reduce_task_list, schedule_plan):
    """Return shuffle thread list (not started)
    
    :param datanodes_address: 
    :param datanode_id_self: 
    :param reduce_task_list: 
    :param schedule_plan
    :return: 
    """
    # shuffle thread list
    shuffle_thread_list = []

    # create socket with each datanode other than self
    for datanode_id, addr_info in datanodes_address.items():
        if int(datanode_id) != datanode_id_self:
            # start shuffle task fetching files from each datanode
            target_datanode_ip = addr_info['ip']
            target_datanode_file_server_port = addr_info['file_server_port']
            # get file in the reduce list for current datanode
            shuffle_thread = threading.Thread(target=thread_shuffle_task, args=(datanode_id, target_datanode_ip, target_datanode_file_server_port, reduce_task_list, schedule_plan, ))
            shuffle_thread_list.append(shuffle_thread)

    return shuffle_thread_list


def do_local_reduce(map_merged_self_dir, reduce_fun, schedule_plan):
    """Return the thread for local reduce (not started)
    
    :param map_merged_self_dir: 
    :param reduce_fun: 
    :param schedule_plan
    :return: 
    """
    local_reduce_thread = threading.Thread(target=thread_local_reduce, args=(map_merged_self_dir, reduce_fun, schedule_plan))
    return local_reduce_thread


def thread_shuffle_task(target_datanode_id, target_datanode_ip, file_server_port, shuffle_task_list, schedule_plan):
    """Create socket and fetch files from datanode
    
    :return: 
    
    """
    global map_merged_dir, datanodes_address, datanode_id_self, datanode_number, final_reduce_queue

    file_client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    file_client_sock.connect((target_datanode_ip, file_server_port))
    file_client_rsock = RSockIO(file_client_sock)

    # make directory for storing map result from datanode
    local_dir_datanode = os.path.join(map_merged_dir, make_datanode_dir_name(target_datanode_id))
    check_and_make_directory(local_dir_datanode)

    for target_partition_id in shuffle_task_list:
        # print("Requesting file " + str(target_partition_id) + " from datanode " + str(target_datanode_id))

        file_request_info = {'type': "FILE_REQUEST", 'partition_id': target_partition_id, "datanode_id": datanode_id_self, 'schedule_plan': schedule_plan}
        send_json(file_client_rsock, file_request_info)
        # get file size info
        file_size_info = get_json(file_client_rsock)
        if file_size_info['type'] == "FILE_SIZE":
            file_size = file_size_info['file_size']
            target_file_path = os.path.join(local_dir_datanode, make_partition_dir_name(target_partition_id))
            get_file(file_client_rsock, target_file_path, file_size)
            if schedule_plan == "NEW":
                final_reduce_partition_queue.put_nowait(target_partition_id)

    file_request_over_info = {'type': "FILE_REQUEST_OVER", "datanode_id": datanode_id_self}
    send_json(file_client_rsock, file_request_over_info)
    file_client_sock.close()


def thread_local_reduce(map_merged_self_dir, reduce_fun, schedule_plan):
    """Thread working on local reduce
    
    :param map_merged_self_dir: 
    :param reduce_fun: 
    :param schedule_plan
    :return: 
    """
    global datanode_number, local_reduce_done_queues, local_reduce_queue, final_reduce_started_queue, shuffle_out_lr_queue
    shuffled_out_list = []
    while True:
        if local_reduce_queue.empty():
            break
        local_reduce_info = local_reduce_queue.get()
        if local_reduce_info['type'] == "LOCAL_REDUCE_PARTITION":
            target_partition_id = local_reduce_info['partition_id']
            if schedule_plan == "NEW":
                if local_reduce_info['to_shuffle']:
                    # partitions to be shuffled, check if already shuffled
                    while not shuffle_out_lr_queue.empty():
                        shuffled_out_partition_id = shuffle_out_lr_queue.get()
                        shuffled_out_list.append(shuffled_out_partition_id)
                    if target_partition_id in shuffled_out_list:
                        continue
                else:
                    # partitions to be reduced, check if already final reduced
                    final_reduce_started_partition_list = final_reduce_started_queue.get()
                    final_reduce_started_queue.put(final_reduce_started_partition_list)
                    # if the target partition id has already started final reduce, no need for local reduce
                    if target_partition_id in final_reduce_started_partition_list:
                        continue

            source_file_path = os.path.join(map_merged_self_dir, make_partition_dir_name(target_partition_id))
            # overwrite source file with reduced file
            reduce_file(source_file_path, source_file_path + "_lr", reduce_fun)

            # print("local Reduce Done " + str(target_partition_id) + " ")

            if schedule_plan == "NEW":
                # after local reduce, notify serve file threads
                for datanode_id in list(range(datanode_number)):
                    local_reduce_done_queues[datanode_id].put_nowait(target_partition_id)
            elif schedule_plan == "ICPP":
                local_reduce_done_lock.acquire()
                local_reduce_done_tracker.append(target_partition_id)
                local_reduce_done_lock.release()


def file_server(file_server_sock):
    """File server for shuffle
    
    :return:
     
    """
    file_server_sock.listen()
    while True:
        sock, addr = file_server_sock.accept()
        rsock = RSockIO(sock)
        serve_file_thread = threading.Thread(target=thread_serve_file, args=(rsock,))
        serve_file_thread.start()


def thread_serve_file(rsock):
    """Thread serving files through socket
    
    :param rsock: 
    :return: 
    
    """
    global map_merged_dir, shuffle_out_lr_queue
    local_reduce_ready_list = []
    while True:
        file_request_info = get_json(rsock)
        request_type = file_request_info['type']
        datanode_id = file_request_info['datanode_id']
        if request_type == "FILE_REQUEST":
            schedule_plan = file_request_info['schedule_plan']
            target_partition_id = file_request_info['partition_id']
            # print("To shuffle out " + str(target_partition_id) + " ")

            # notify that the partition id is already shuffled out, no need for local reduce (under NEW)
            shuffle_out_lr_queue.put_nowait(target_partition_id)

            # check whether target partition has already been reduced, if so, send reduced file
            if schedule_plan == 'NEW':
                target_partition_file_path = os.path.join(map_merged_self_dir, make_partition_dir_name(target_partition_id))
                while not local_reduce_done_queues[datanode_id].empty():
                    local_reduce_ready_partition = local_reduce_done_queues[datanode_id].get()
                    local_reduce_ready_list.append(local_reduce_ready_partition)
                if target_partition_id in local_reduce_ready_list:
                    target_partition_file_path = target_partition_file_path + "_lr"
                # while True:
                #     if target_partition_id in local_reduce_ready_list:
                #         break
                #     local_reduce_ready_partition = local_reduce_done_queues[datanode_id].get()
                #     local_reduce_ready_list.append(local_reduce_ready_partition)
                # target_partition_file_path = os.path.join(map_merged_self_dir, make_partition_dir_name(target_partition_id))
                # target_partition_file_path = target_partition_file_path + '_lr'

            elif schedule_plan == 'ICPP':
                target_partition_file_path = os.path.join(map_merged_self_dir, make_partition_dir_name(target_partition_id))
                local_reduce_done_lock.acquire()
                local_reduce_done_list = local_reduce_done_tracker
                local_reduce_done_lock.release()
                if target_partition_id in local_reduce_done_list:
                    target_partition_file_path = target_partition_file_path + '_lr'

            else:
                target_partition_file_path = os.path.join(map_merged_self_dir, make_partition_dir_name(target_partition_id))

            # send file size information
            file_size_info = {"type": "FILE_SIZE", "file_size": os.stat(target_partition_file_path).st_size}
            send_json(rsock, file_size_info)

            # send file
            send_file(rsock, target_partition_file_path)
        elif request_type == "FILE_REQUEST_OVER":
            # if no more requests, end
            rsock.close_sock()
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


def thread_final_reduce(reduce_fun, map_merged_dir, map_merged_final_dir, reduce_output_datanode_dir, reduce_task_list, job_schedule_plan):
    """Thread fetches final reduce task from queue and do final reduce
    
    :return: 
    """
    global final_reduce_queue, datanode_number, final_reduce_partition_queue, final_reduce_started_queue
    final_reduce_progress_counter = len(reduce_task_list)

    if job_schedule_plan == 'NEW':
        shuffle_in_progress = dict()
        for partition_id in reduce_task_list:
            shuffle_in_progress[partition_id] = 0

        while True:
            shuffle_in_partition = final_reduce_partition_queue.get()
            shuffle_in_progress[shuffle_in_partition] += 1
            if shuffle_in_progress[shuffle_in_partition] == datanode_number - 1:

                # update already started final reduce partition list
                started_final_reduce_partition = final_reduce_started_queue.get()
                started_final_reduce_partition.append(shuffle_in_partition)
                final_reduce_started_queue.put(started_final_reduce_partition)

                # merge and final reduce
                merge_map_output_final_partition(map_merged_dir, map_merged_final_dir, shuffle_in_partition, datanode_number)
                final_reduce_partition(map_merged_final_dir, reduce_output_datanode_dir, reduce_fun, shuffle_in_partition)

                # check whether all final reduce tasks done
                final_reduce_progress_counter -= 1
                if final_reduce_progress_counter == 0:
                    break
    else:
        while True:
            info = final_reduce_queue.get()
            if info['type'] == "FINAL_REDUCE_PARTITION":
                partition_id = info['partition_id']
                # print("Final reduce partition id " + str(partition_id)+ " ")
                # first do final merge
                merge_map_output_final_partition(map_merged_dir, map_merged_final_dir, partition_id, datanode_number)

                # final reduce on this partition
                final_reduce_partition(map_merged_final_dir, reduce_output_datanode_dir, reduce_fun, partition_id)

                # check if all done, if so, end thread
                final_reduce_progress_counter -= 1

            if final_reduce_progress_counter == 0:
                break

if __name__ == "__main__":
    datanode_start()
