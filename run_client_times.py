from client import *

times_each = 5

schedule_plans = ['NEW', 'ICPP', 'HADOOP']


def start():
    for schedule_plan in schedule_plans:
        for i in list(range(times_each)):
            print("Starting schedule: " + schedule_plan)
            print("For the " + str(i + 1) + "th time")
            client_start(schedule_plan)

if __name__ == "__main__":
    start()
