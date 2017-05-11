import socket
from config import *
from tools import *
from robust_socket_io import *
from sys import argv
import time

default_plan = "HADOOP"

record_dir_path = os.path.join(".", "job_record")

partition_number = task_config['partition_number']

datanode_number = general_config['datanode_number']


def client_start(schedule_plan, input_volume=768):
    """client for submitting jobs
    
    :param schedule_plan
    :param input_volume
    :return: 
    
    """
    # connect to namenode
    namenode_ip = net_config['namenode_ip']
    namenode_port_out = net_config['namenode_port_out']
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((namenode_ip, namenode_port_out))
    rsock = RSockIO(sock)

    job_fs_path = os.path.join("wordcount", str(input_volume))

    # send job information
    job_information = {"type": "NEW_JOB", "job_name": "WordCount", "job_fs_path": job_fs_path, "schedule_plan": schedule_plan}
    send_json(rsock, job_information)

    # receive job feedback
    # when receive status "JOB_DONE" or "ERROR", terminate
    job_record_info = dict()
    job_id = int(time.time())
    job_record_info['schedule_plan'] = schedule_plan
    job_record_info['map_time'] = []
    job_record_info['exec_schedule_time'] = []
    job_record_info['datanode_job_time'] = []
    job_record_info['namenode_job_time'] = None
    job_record_info['partition_number'] = partition_number
    job_record_info['datanode_number'] = datanode_number
    job_record_info['input_volume'] = input_volume
    while True:
        job_feedback = get_json(rsock)
        if job_feedback['status'] == "ERROR":
            err_log(job_feedback['status'], job_feedback['message'])
            break
        else:
            if job_feedback['status'] == "schedule_feedback":
                # print(job_feedback)
                job_record_info['partition_info'] = job_feedback['partition_info']
                job_record_info['reduce_task_lists'] = job_feedback['reduce_task_lists']
            elif job_feedback['status'] == "JOB_DONE":
                log(job_feedback['status'], job_feedback['message'])
                break
            else:
                log(job_feedback['status'], job_feedback['message'])

    # receive time feedback
    while True:
        time_feedback = get_json(rsock)
        if time_feedback['status'] == 'TIME_FEEDBACK_DONE':
            break
        elif time_feedback['status'] == 'TIME_FEEDBACK':
            time_info = time_feedback['time_info']
            job_record_info['map_time'].append(time_info['map'])
            job_record_info['exec_schedule_time'].append(time_info['exec_schedule'])
            job_record_info['datanode_job_time'].append(time_info['datanode_job'])
            job_record_info['namenode_job_time'] = time_info['namenode_job']
            # print(time_info)
    # close socket
    rsock.close_sock()

    if not os.path.exists(record_dir_path):
        err_log("FS", "job record dir not ready, record file not written")
    else:
        # write job record to local file
        with open(os.path.join(record_dir_path, str(job_id)), 'w', encoding='utf-8') as f:
            f.write(str(job_record_info))

if __name__ == "__main__":
    if len(argv) == 2:
        script, plan = argv
        if plan in ['HADOOP', "ICPP", "NEW"]:
            client_start(plan)
        else:
            log("ERROR", "schedule plan " + plan + " not known, using default plan")
            client_start(default_plan)
    else:
        client_start(default_plan)
