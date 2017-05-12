import os

# general config
general_config = {
    "datanode_number": 8,
}


# net-related config
net_config = {
    "namenode_ip": "45.32.46.113",
    "namenode_port_in": 1112,
    "namenode_port_out": 2212,
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
    "partition_number": 80,
}