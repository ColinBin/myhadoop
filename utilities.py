from itertools import groupby
from config import task_config
import os

partition_number = task_config['partition_number']


def partition_sorted(partition_sorted_data, map_task_dir):
    """Make partitions based on sorted data
    Return length information for each partition
    :param partition_sorted_data
    :param map_task_dir: 
    :return: partition info
    
    """
    partition_info = dict()
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
