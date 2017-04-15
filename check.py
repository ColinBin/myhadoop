import os
from config import *
from tools import *
from shutil import rmtree


def check_dir(job_fs_path):
    """Check input and output directories
    
    Check input files for app. 
    When the output directory already exists, overwrite or exit depending on output_overwrite.
    :return: (app_input_dir, input_file_list) 
    
    """
    log("FS", "checking input & output directories")

    if not os.path.exists(fs_config['input_dir']):
        err_log("FS", "input directory not ready")
    if not os.path.exists(fs_config['output_dir']):
        err_log("FS", "output_directory not ready")

    app_input_dir = fs_config['input_dir'] + "/" + job_fs_path
    app_output_dir = fs_config['output_dir'] + "/" + job_fs_path

    if not os.path.exists(app_input_dir):
        err_log("FS", "app input directory not ready")
    else:
        input_file_list = [f for f in os.listdir(app_input_dir) if os.path.isfile(os.path.join(app_input_dir, f))]
        if len(input_file_list) <= 0:
            err_log("FS", "no input files for " + job_fs_path)
    if os.path.exists(app_output_dir):
        if fs_config['output_overwrite'] is True:
            rmtree(app_output_dir)
            os.mkdir(app_output_dir, 0o755)
        else:
            err_log("FS", "output directory for " + job_fs_path + " already exists")
    log("FS", "input & output directories ready")
    return app_input_dir, app_output_dir, input_file_list

if __name__ == "__main__":
    check_dir("wordcount")

