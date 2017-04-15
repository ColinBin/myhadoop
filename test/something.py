import json
import queue
from itertools import groupby

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

