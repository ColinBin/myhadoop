import json

d = dict()
d[1] = 1
print(d[1])

data_json = {"name": "Colin", "age": 18}
data_bytes = json.dumps(data_json).encode()
data_json = json.loads(data_bytes)
print(data_json["name"])
