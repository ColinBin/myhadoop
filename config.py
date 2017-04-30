# general config
general_config = {
    "mode": "local",
    "datanode_number": 3,
}


# net-related config
net_config = {
    "namenode_ip": "localhost",
    "namenode_port_in": 6666,
    "namenode_port_out": 8888,
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
    "partition_number": 15,
}