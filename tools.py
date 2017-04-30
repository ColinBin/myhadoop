import sys
import json
import os
import shutil
from config import *
from robust_socket_io import *


def err_log(err_type, err_message):
    """Print err information to stderr
    
    :param err_type: 
    :param err_message: 
    :return: None
    
    """
    print("[" + err_type + "] " + err_message, file=sys.stderr)


def log(log_type, log_message):
    """Print log information to stdout
    
    :param log_type: 
    :param log_message: 
    :return: None
    
    """
    print("[" + log_type + "] " + log_message, flush=True)


def log_time(activity, time):
    """Print time elapsed
    
    :param activity: 
    :param time: 
    :return: 
    """
    time_formatted = int(time * 1000) / 1000
    log("TIME", activity + " took " + str(time_formatted) + " seconds")


def get_json(rsock):
    """Get json from sock
    
    :param rsock: 
    :return: json object
     
    """
    received_length, data_str = rsock.readlineb()
    data_json = json.loads(data_str)
    return data_json


def send_json(rsock, data_json):
    """Send json data after converting to bytes
    
    :param rsock:
    :param data_json: 
    :return: 
    
    """
    data_bytes = json.dumps(data_json).encode()
    rsock.sendline(data_bytes)


def send_file(rsock, file_path):
    """Send file and check echo data
    
    :param rsock: 
    :param file_path: 
    :return: 
    
    """

    with open(file_path, 'r', encoding='utf-8') as f:
        file_content = f.read()
    rsock.sendn(file_content.encode())


def get_file(rsock, target_file_path, file_size):
    """Get file from sock and write to target_path
    
    :param rsock: 
    :param target_file_path: 
    :param file_size: 
    :return: 
    
    """
    received_length, file_content = rsock.readnb(file_size)
    if received_length != file_size:
        err_log("FS", "lost integrity of file ")

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


def make_datanode_dir_name(datanode_id):
    return "datanode_" + str(datanode_id)


def make_partition_dir_name(partition_id):
    return "partition_" + str(partition_id)


def check_dir_for_job(job_fs_path):
    """Check input and output directories

    Check input files for app. 
    When the output directory already exists, overwrite or exit depending on output_overwrite.
    :return: (app_input_dir, input_file_list) 

    """
    log("FS", "checking input & output directories")

    if not os.path.exists(fs_config['input_dir']):
        err_log("FS", "input directory not ready")
    # if not os.path.exists(fs_config['output_dir']):
    #     err_log("FS", "output_directory not ready")

    app_input_dir = fs_config['input_dir'] + "/" + job_fs_path
    app_output_dir = fs_config['output_dir'] + "/" + job_fs_path

    if not os.path.exists(app_input_dir):
        err_log("FS", "app input directory not ready")
    else:
        input_file_list = [f for f in os.listdir(app_input_dir) if os.path.isfile(os.path.join(app_input_dir, f))]
        if len(input_file_list) <= 0:
            err_log("FS", "no input files for " + job_fs_path)
            # if os.path.exists(app_output_dir):
            #     if fs_config['output_overwrite'] is True:
            #         rmtree(app_output_dir)
            #         os.mkdir(app_output_dir, 0o755)
            #     else:
            err_log("FS", "output directory for " + job_fs_path + " already exists")
    # log("FS", "input & output directories ready")
    return app_input_dir, app_output_dir, input_file_list