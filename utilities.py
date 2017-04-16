from itertools import groupby
from config import task_config
from tools import *
import os

partition_number = task_config['partition_number']


class LocalityHeap(object):
    def __init__(self):
        pass


class NodeHeap(object):
    def __init__(self):
        pass


def partition_sorted(partition_sorted_data, map_task_dir):
    """Make partitions based on sorted data
    Return length information for each partition
    :param partition_sorted_data
    :param map_task_dir: 
    :return: partition info
    
    """
    partition_info = dict()

    # in case no data for some partition id, set 0 as default
    for partition_id in list(range(partition_number)):
        partition_info[partition_id] = 0

    for partition_id, per_group_data in groupby(partition_sorted_data, key=get_key_for_sort_partition):
        with open(os.path.join(map_task_dir, str(partition_id)), 'w', encoding='utf-8') as f:
            final_sorted_group_data_list = list(sorted(per_group_data, key=get_key_for_sort_normal))
            partition_info[partition_id] = len(final_sorted_group_data_list)
            f.write(str(final_sorted_group_data_list))
    return partition_info


def get_key_for_sorted(data):
    """Return key of each data for function sorted

    :param data: 
    :return: the key

    """
    return data[0]


def get_key_for_sort_normal(data):
    """get_key function for groupby
    
    :param data: 
    :return: 
    """
    return data[0]


def get_key_for_sort_partition(data):
    """Based on the key, return partition id

    :param data: 
    :return: 
    """
    key = data[0]
    global partition_number
    if isinstance(key, str):
        sum_of_ascii = sum(bytearray(key.encode()))
        return sum_of_ascii % partition_number
    else:
        return 0


def merge_partition_info(current_info, new_info):
    """Merge partition information
    
    :param current_info: 
    :param new_info: 
    :return: 
    
    """
    for partition_id in list(range(partition_number)):
        current_info[partition_id] = current_info.get(partition_id, 0) + new_info[str(partition_id)]


def merge_map_output(local_dir, target_dir, partition_number):
    """Merge map tasks output and put in target directory
    
    :param target_dir
    :param local_dir: 
    :param partition_number: 
    :return:
     
    """
    # record task path for each map task
    map_task_dirs = [os.path.join(local_dir, map_task_dir) for map_task_dir in os.listdir(local_dir) if os.path.isdir(os.path.join(local_dir, map_task_dir))]

    # make target dir
    check_and_make_directory(target_dir)

    for partition_id in list(range(partition_number)):
        # merge for each partition
        current_target_file = os.path.join(target_dir, str(partition_id))

        # TODO write specialized merge function to merge files
        # get and sort is less efficient
        # get content from all tasks and sort
        content_current_partition = []
        for map_task_dir in map_task_dirs:
            map_task_partition_file = os.path.join(map_task_dir, str(partition_id))
            with open(map_task_partition_file, 'r', encoding='utf-8') as f:
                content_list = eval(f.read())
                content_current_partition += content_list

        sorted_content = sorted(content_current_partition, key=get_key_for_sort_normal)
        with open(current_target_file, 'w', encoding='utf-8') as f:
            f.write(str(sorted_content))