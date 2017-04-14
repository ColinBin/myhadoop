
from functools import reduce


def wordcount_map(word):
    return word, 1


def wordcount_reduce(record1, record2):
    return record1[0], record1[1] + record2[1]


def get_key(y):
    return y[0]


list_str = ["hello", 'Worldx', 'Colin', "World", 'Colin']

map_result = map(wordcount_map, list_str)
sorted_result = sorted(map_result, key=get_key)

record_keeper = dict()
last_key = None
record_count = 0

for record in sorted_result:
    current_key = get_key(record)
    if (current_key != last_key) & (last_key is not None):
        record_keeper[last_key] = record_count
        record_count = 0
    else:
        record_count += 1

print(sorted_result)
reduce_result = reduce(wordcount_reduce, sorted_result)
print(list(reduce_result))

