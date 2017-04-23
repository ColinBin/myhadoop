from itertools import groupby
from config import task_config
from tools import *
import os
from functools import reduce

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
        with open(os.path.join(map_task_dir, make_partition_dir_name(partition_id)), 'w', encoding='utf-8') as f:
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


def merge_files(source_file_list, target_file_path, comparator):
    """Merge files in the list while maintaining the order of the elements
    
    :param source_file_list: 
    :param target_file_path: 
    :param comparator: 
    :return: 
    
    """
    # TODO open multiple files at the same time or pull the content out of each file
    source_file_number = len(source_file_list)      # number of files to be processed
    file_finished_number = 0                        # number of files currently finished
    output_content = []                             # output to write
    source_file_index = dict()
    for file_id in list(range(source_file_number)):
        source_file_index[file_id] = 0
    source_file_length = [0 for file_id in list(range(source_file_number))]
    source_file_content = [[] for file_id in list(range(source_file_number))]
    for file_id in list(range(source_file_number)):
        with open(source_file_list[file_id], 'r', encoding='utf-8') as f:
            current_file_content = eval(f.read())
            source_file_length[file_id] = len(current_file_content)
            source_file_content[file_id] = current_file_content

    current_candidates = [(file_id, source_file_content[file_id][0]) for file_id in list(range(source_file_number))]
    while file_finished_number < source_file_number:
        minimal_file_index, content = find_minimal(current_candidates, comparator)
        output_content.append(content)


    # write final output
    with open(target_file_path, 'w', encoding='utf-8') as f:
        f.write(str(output_content))

def find_minimal(candidates, comparator):
    candidate_id = 0
    content = candidates[candidate_id]
    for candidate in candidates:
        if comparator(candidate[1], content) < 0:
            candidate_id = candidate[0]
            content = candidate[1]
    return candidate_id, content


def merge_and_sort(source_file_list, target_file_path):
    """Merge source files in the list, sort the combined content and write to the target file path
    
    :param source_file_list: 
    :param target_file_path: 
    :return: 
    """
    content = []
    file_number = len(source_file_list)
    for file_id in list(range(file_number)):
        with open(source_file_list[file_id], 'r', encoding='utf-8') as f:
            content_list = eval(f.read())
            content = content + content_list
    sorted_content = sorted(content, key=get_key_for_sort_normal)
    with open(target_file_path, 'w', encoding='utf-8') as f:
        f.write(str(sorted_content))


def merge_map_output(local_dir, map_merged_dir, target_dir, partition_number):
    """Merge map tasks output and put in target directory
    
    :param map_merged_dir
    :param target_dir
    :param local_dir: 
    :param partition_number: 
    :return:
     
    """
    # record task path for each map task
    map_task_dirs = [os.path.join(local_dir, map_task_dir) for map_task_dir in os.listdir(local_dir) if os.path.isdir(os.path.join(local_dir, map_task_dir)) and os.path.normpath(os.path.join(local_dir, map_task_dir)) != os.path.normpath(map_merged_dir)]

    # make target dir
    check_and_make_directory(target_dir)

    for partition_id in list(range(partition_number)):
        # merge for each partition
        current_target_file = os.path.join(target_dir, make_partition_dir_name(partition_id))
        source_file_list = [os.path.join(map_task_dir, make_partition_dir_name(partition_id)) for map_task_dir in map_task_dirs]
        merge_and_sort(source_file_list, current_target_file)


def merge_map_output_final(source_dir, target_dir, reduce_task_list, datanode_number):
    """Merge local map result and files from other datanodes
    :param datanode_number
    :param source_dir: 
    :param target_dir: 
    :param reduce_task_list: 
    :return:
     
    """
    for current_partition_id in reduce_task_list:
        target_file_path = os.path.join(target_dir, make_partition_dir_name(current_partition_id))
        source_file_list = [os.path.join(source_dir, make_datanode_dir_name(datanode_id), make_partition_dir_name(current_partition_id)) for datanode_id in list(range(datanode_number))]
        merge_and_sort(source_file_list, target_file_path)


def final_reduce(map_merged_final_dir, reduce_output_datanode_dir, reduce_fun, reduce_task_list):
    """Final reduce using final merged files, write to reduce output dir
    
    :param map_merged_final_dir: 
    :param reduce_output_datanode_dir: 
    :param reduce_fun: 
    :param reduce_task_list
    :return: 
    
    """
    for target_partition_id in reduce_task_list:
        target_file_path = os.path.join(reduce_output_datanode_dir, make_partition_dir_name(target_partition_id))

        merged_file = os.path.join(map_merged_final_dir, make_partition_dir_name(target_partition_id))
        reduce_result = []
        with open(merged_file, 'r', encoding='utf-8') as f:
            file_content_list = eval(f.read())
        for key, group in groupby(file_content_list, key=get_key_for_sort_normal):
            reduce_result.append(reduce(reduce_fun, list(group)))
        with open(target_file_path, 'w', encoding='utf-8') as f:
            f.write(str(reduce_result))
