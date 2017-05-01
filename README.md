Run "systemctl stop firewalld" before running datanodes, otherwise "no route to host" of errno 113 will block shuffling.

Do not run any datanode on the same machine as namenode when running as distributed mode.