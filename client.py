import socket
from config import *
from tools import *
from robust_socket_io import *


def client_start():
    """client for submitting jobs
    
    :return: 
    
    """
    # connect to namenode
    namenode_ip = net_config['namenode_ip']
    namenode_port_out = net_config['namenode_port_out']
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((namenode_ip, namenode_port_out))
    rsock = RSockIO(sock)

    # send job information
    job_information = {"type": "NEW_JOB", "job_name": "WordCount", "job_fs_path": "wordcount"}
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
    client_start()
