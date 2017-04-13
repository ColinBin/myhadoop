# general config
general_config = {
    "mode": "local",
    "datanode_number": 4,
    "app_name": "wordcount",
}


# net-related config
net_config = {
    "namenode_ip": "localhost",
    "namenode_port_in": 6666,
    "namenode_port_out": 8888,
    # "scheduler_ip": "localhost",
    # "scheduler_port": 8888,
}

# file system config
fs_config = {
    "input_dir": "./fs/input",
    "output_dir": "./fs/output",
    "output_overwrite": True,
}