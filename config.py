# general config
general_config = {
    "mode": "local",
    "datanode_number": 2,
    "app_name": "wordcount",
}


# net-related config
net_config = {
    "namenode_ip": "45.32.46.113",
    "namenode_port_in": 1114,
    "namenode_port_out": 2225,
}

# file system config
fs_config = {
    "datanode_dir": "./fs/datanode",
    "input_dir": "./fs/input",
    "output_dir": "./fs/output",
    "output_overwrite": True,
}

# task config
task_config = {
    "partition_number": 10,
}