import config

partition_number = config.task_config['partition_number']


class WordCount(object):
    def __init__(self):
        pass

    def map(self, word):
        return word, 1

    def reduce(self, record1, record2):
        return record1[0], int(record1[1]) + int(record2[1])


class MergeSort(object):
    def __init__(self):
        pass

    def map(self, item):
        return item, 1

    def reduce(self, record1, record2):
        len1 = len(record1)
        len2 = len(record2)
        index1 = 0
        index2 = 0
        result = []
        while index1 < len1 and index2 < len2:
            if record1[index1][0] < record2[index2][0]:
                result.append(record1[index1])
                index1 += 1
            else:
                result.append(record2[index2])
                index2 += 1
        if index1 < len1:
            for i in list(range(index1, len1)):
                result.append(record1[index1])
        else:
            for i in list(range(index2, len2)):
                result.append(record2[index2])
        return result

