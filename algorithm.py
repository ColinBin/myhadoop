from heapq import *


def get_combined_locality(internal_locality, node_locality):
    """Return combined-locality based on node and internal locality
    
    :return: 
    """
    return internal_locality * node_locality


def heap_schedule(partition_info_tracker, combined_locality):
    datanode_number = len(combined_locality)
    partition_number = len(combined_locality[0])

    datanode_heap = []
    locality_heaps = []
    reduce_decisions = dict()
    partition_locality = []

    # initialize heaps
    for datanode_id in list(range(datanode_number)):
        # datanode workload : datanode id
        heappush(datanode_heap, (0, datanode_id))
        locality_heap = []
        for partition_id in list(range(partition_number)):
            # combined locality : partition id
            # max heap
            heappush(locality_heap, (combined_locality[datanode_id][partition_id] * (-1), partition_id))
        locality_heaps.append(locality_heap)

    partition_assigned_counter = 0

    while partition_assigned_counter != partition_number:
        # find the datanode with the minimum workload first
        datanode_info = heappop(datanode_heap)
        current_datanode_id = datanode_info[1]

        # get the heap storing locality information of current datanode
        locality_heap = locality_heaps[current_datanode_id]

        # find the first partition not assigned
        while True:
            locality_info = heappop(locality_heap)
            current_partition_id = locality_info[1]
            if current_partition_id not in reduce_decisions.keys():
                break

        # update partition_locality with original locality value
        partition_locality.append((current_partition_id, locality_info[0] * (-1)))

        # update workload information and datanode heap
        new_datanode_info = (datanode_info[0] + sum(partition_info_tracker[current_datanode_id][current_partition_id] for datanode_id in list(range(datanode_number))), current_datanode_id)
        heappush(datanode_heap, new_datanode_info)

        # update indicators
        partition_assigned_counter += 1
        reduce_decisions[current_partition_id] = current_datanode_id

    return reduce_decisions, partition_locality
