import os

record_dir_path = os.path.join(".", "job_record")
formatted_record_path = os.path.join(record_dir_path, "formatted_record")
target_file_path = os.path.join(formatted_record_path, "record")


def start():
    record_files = [os.path.join(record_dir_path, f) for f in os.listdir(record_dir_path) if os.path.isfile(os.path.join(record_dir_path, f))]
    record_lines = []
    for record_file in record_files:
        record_line = ""
        with open(record_file, 'r', encoding='utf-8') as f:
            data_dict = eval(f.read())

        # schedule plan
        schedule_plan = data_dict['schedule_plan']
        record_line = add_item(record_line, schedule_plan)

        # map task time
        map_time_list = data_dict['map_time']
        for map_time in map_time_list:
            record_line = add_item(record_line, map_time)

        # exec schedule time
        exec_schedule_time_list = data_dict['exec_schedule_time']
        for exec_schedule_time in exec_schedule_time_list:
            record_line = add_item(record_line, exec_schedule_time)

        # datanode job time
        datanode_job_time_list = data_dict['datanode_job_time']
        for datanode_job_time in datanode_job_time_list:
            record_line = add_item(record_line, datanode_job_time)

        # namenode job time
        namenode_job_time = data_dict['namenode_job_time']
        record_line = add_item(record_line, namenode_job_time)

        record_lines.append(record_line)

    with open(target_file_path, 'w', encoding='utf-8') as f:
        for record_line in record_lines:
            f.write(record_line)
            f.write("\n")


def add_item(line, to_add):
    return line + str(to_add) + "\t"

if __name__ == "__main__":
    start()