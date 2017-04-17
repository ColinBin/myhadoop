import json
import queue
from itertools import groupby
import os

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

h = [("Hello", 1), ("Hello", 1), ("Jack", 1), ("Jack", 1), ("Moon", 1), ("Moon", 1)]
for k, g in groupby(h, key=keyfunc):
    print(k)
    print(len(list(g)))

print(isinstance('Colin', str))

print(sum(bytearray(b"A")))

l = list("[1, 2, 3, 4]")
print(h)

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
