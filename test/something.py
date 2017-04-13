import json

d = dict()
d[1] = 1
print(d[1])

data_json = {"name": "Colin", "age": 18}
data_bytes = json.dumps(data_json).encode()
data_json = json.loads(data_bytes)
print(data_json["name"])

print(["Hello" for i in list(range(1, 6))])

s = [1, 2, 3]
print(s[1])


print(list(range(5)))
