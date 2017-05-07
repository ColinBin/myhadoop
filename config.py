import os

# general config
general_config = {
    "datanode_number": 4,
}


# net-related config
net_config = {
    "namenode_ip": "localhost",
    "namenode_port_in": 6666,
    "namenode_port_out": 8888,
}

# file system config
fs_config = {
    "datanode_dir": os.path.join(".", 'fs', "datanode"),
    "input_dir": os.path.join(".", "fs", "input"),
    "output_dir": os.path.join(".", "fs", "output"),
    "output_overwrite": True,
}

# task config
task_config = {
    "partition_number": 20,
}