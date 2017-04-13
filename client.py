import socket
import json
from config import *
from tools import *


def client_start():
    """client for submitting jobs
    
    :return: 
    
    """
    # connect to namenode
    namenode_ip = net_config['namenode_ip']
    namenode_port_out = net_config['namenode_port']
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((namenode_ip, namenode_port_out))

    # send job information
    job_information = {"request_type": "NEW_JOB", "job_name": "WordCount", "job_fs_path": "wordcount"}
    job_to_send = json.dump(job_information).encode()
    sock.send(job_to_send)

    # receive job feedback
    # when receive status "FINISH" or "ERROR", terminate
    while True:
        job_feedback = get_json(sock)
        if job_feedback['status'] == "ERROR":
            err_log(job_feedback['status'], job_feedback['message'])
            break
        else:
            log(job_feedback['status'], job_feedback['message'])
            if job_feedback['status'] == "FINISH":
                break

    # close socket
    sock.close()


if __name__ == "__main__":
    client_start()
