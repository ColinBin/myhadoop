from client import *
from tools import get_sequence_suffix

times_each = 3

schedule_plans = ['HADOOP', 'ICPP', "NEW"]

input_volume_plans = [512, 768, 1024, 2048]


def start():
    for schedule_plan in schedule_plans:
        for input_volume in input_volume_plans:
            for i in list(range(times_each)):
                print("Schedule: " + schedule_plan + "\r" + "volume: " + str(input_volume))
                print("For the " + get_sequence_suffix(i + 1) + " time")
                client_start(schedule_plan, input_volume)

if __name__ == "__main__":
    start()
