import json
import queue
from itertools import groupby
import os
import time
class App(object):

    def map(self):
        print("Calling map function")

    def reduce(self, name):
        print(name + "Calling reduce function")


def keyfunc(data):
    return data[0]


def hello(name):
    print("Hello " + name)

f = dict()
f['first'] = App

f['first']().reduce("Colin")



print(isinstance('Colin', str))

print(sum(bytearray(b"A")))

l = list("[1, 2, 3, 4]")

n = dict()
n['name'] = "Colin"
n[2] = "Colin"
while True:
    print("Here")
    for v in n.values():
        if v != "Colin":
            break
    else:
        break
n[3] = n.get(3, 32) + 3
print(n[3])

a = [1,2,3]
b = [4,5,6]
print(a + b)

local_path = "."
io_path = os.path.join(local_path, "io")
dirs = [d for d in os.listdir(local_path) if os.path.isdir(os.path.join(local_path, d)) and os.path.normpath(os.path.join(local_path, d)) != os.path.normpath(io_path)]
print(dirs)

di = {"1":"fsfs", "31": [2,3,4], 213:{"1":323, "2":"fafaf"}}
print(len(di))
print(os.path.join(".", "input", "partition1"))


h = [("Hello", 1), ("Hello", 1), ("Jack", 1), ("Jack", 1), ("Moon", 1), ("Moon", 1)]
for k, g in groupby(h, key=keyfunc):
    print(k)
    print(list(g))


to_send = {"name": "Jack", "age": 22}
fin = json.dumps(to_send).encode() + b'\n' * (50 - len(json.dumps(to_send).encode()))
print(json.dumps(to_send).encode())
print(fin)
print(len(fin))
json_data = json.loads(fin.decode('utf-8'))
print(isinstance(json_data, dict))
print(json_data['name'])

stat_info = os.stat("something.py")
print(stat_info.st_size)

print(isinstance(b"faf", bytes))

a = [3]
a.remove(3)
if a:
    print("dfahjfksafja")

print((1,2,3) == (1,2,3))

from collections import deque
task_list = [1, 4, 3, 6]
td = deque(task_list)
print(td.popleft())
if td:
    print("Yes")
print(td.pop())
print(td.pop())
print(td.popleft())
if td:
    print("Yes")

print(time.time())

