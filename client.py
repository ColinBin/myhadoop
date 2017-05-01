import socket
from config import *
from tools import *
from robust_socket_io import *
from sys import argv

default_plan = "HADOOP"


def client_start(schedule_plan):
    """client for submitting jobs
    
    :param schedule_plan
    :return: 
    
    """
    # connect to namenode
    namenode_ip = net_config['namenode_ip']
    namenode_port_out = net_config['namenode_port_out']
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((namenode_ip, namenode_port_out))
    rsock = RSockIO(sock)

    # send job information
    job_information = {"type": "NEW_JOB", "job_name": "WordCount", "job_fs_path": "wordcount", "schedule_plan": schedule_plan}
    send_json(rsock, job_information)

    # receive job feedback
    # when receive status "JOB_DONE" or "ERROR", terminate
    while True:
        job_feedback = get_json(rsock)
        if job_feedback['status'] == "ERROR":
            err_log(job_feedback['status'], job_feedback['message'])
            break
        else:
            log(job_feedback['status'], job_feedback['message'])
            if job_feedback['status'] == "JOB_DONE":
                break

    # close socket
    rsock.close_sock()


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
