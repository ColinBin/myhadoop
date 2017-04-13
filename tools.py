import sys
import json

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


def get_json(sock, max_length=1024):
    """Get json from sock
    
    :param sock: 
    :param max_length: 
    :return: json object
     
    """
    data_bytes = sock.recv(max_length)
    data_json = json.loads(data_bytes)
    return data_json


def send_json(sock, data_json):
    """Send json data after converting to bytes
    
    :param sock:
    :param data_json: 
    :return: 
    
    """
    data_bytes = json.dumps(data_json).encode()
    sock.send(data_bytes)
