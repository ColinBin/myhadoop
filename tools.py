import sys
import json
import os
import shutil


def err_log(err_type, err_message):
    """Print err information to stderr
    
    :param err_type: 
    :param err_message: 
    :return: None
    
    """
    print(err_type + ": " + err_message, file=sys.stderr)


def log(log_type, log_message):
    """Print log information to stdout
    
    :param log_type: 
    :param log_message: 
    :return: None
    
    """
    print(log_type + ": " + log_message, flush=True)


def get_json(sock, max_length=2048):
    """Get json from sock
    
    :param sock: 
    :param max_length: 
    :return: json object
     
    """
    data_bytes = sock.recv(max_length)
    data_json = json.loads(data_bytes.decode('utf-8'))
    return data_json


def send_json(sock, data_json):
    """Send json data after converting to bytes
    
    :param sock:
    :param data_json: 
    :return: 
    
    """
    data_bytes = json.dumps(data_json).encode()
    sock.sendall(data_bytes)


def check_echo_success(sock):
    """Check echo information from datanodes
    
    :param sock: 
    :return: 
    
    """
    echo_data = get_json(sock)
    if echo_data['type'] is "ECHO" and echo_data['status'] is not "SUCCESS":
        err_log("TASK", "failed to start task ")


def send_echo_success(sock):
    """Send echo data to namenode for each task
    
    :param sock: 
    :return: 
    
    """
    echo_data = {'type': "ECHO", "status": "SUCCESS"}
    send_json(sock, echo_data)


def get_json_echo(sock):
    """Get json data and send echo
    
    :param sock: 
    :return: json_data 
    
    """
    json_data = get_json(sock)
    send_echo_success(sock)
    return json_data


def send_json_check_echo(sock, json_data):
    """Send json data and check echo data
    
    :param sock: 
    :param json_data: 
    :return:
     
    """
    send_json(sock, json_data)
    check_echo_success(sock)


def send_file(sock, file_path):
    """Send file and check echo data
    
    :param sock: 
    :param file_path: 
    :return: 
    
    """

    with open(file_path, 'r', encoding='utf-8') as f:
        file_content = f.read()
    sock.sendall(file_content.encode())


def get_file(sock, target_file_path, file_size):
    """Get file from sock and write to target_path
    
    :param sock: 
    :param target_file_path: 
    :param file_size: 
    :return: 
    
    """
    # TODO use robust io
    received_length = 0
    file_content = ""
    buff_size = 4096
    while received_length != file_size:
        part = sock.recv(buff_size).decode('utf-8')
        file_content = file_content + part
        received_length += len(part)

    with open(target_file_path, 'w', encoding='utf-8') as f:
        f.write(file_content)


def check_and_make_directory(dir_path):
    """Check whether dir_path exists, if exists, remove and remake 
    
    :param dir_path: 
    :return: 
    """
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.mkdir(dir_path, 0o755)

