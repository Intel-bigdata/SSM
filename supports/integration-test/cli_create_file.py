import sys
from util import *


def create_file_CLI(dir_num):
    """
    Please use this script in namenode
    Each time create 10K * 2 files (10K in io_data and 10K in io_control).
    Then, move these data to TEST_DIR.
    """
    for i in range(dir_num):
        file_index = 0
        dir_name = TEST_DIR + random_string()
        # Create dir
        subprocess.call("hdfs dfs -mkdir " + dir_name, shell=True)
        command_arr = []
        for i in range(10000 / dir_num):
            # run create file command in parallel
            command_arr.append("hdfs dfs -touchz " +
                               dir_name + "/" + str(file_index))
            file_index += 1
        exec_commands(command_arr)


if __name__ == '__main__':
    num = 50
    try:
        num = int(sys.argv[1])
    except ValueError:
        print "Usage: python cli_create_file [num]"
    except IndexError:
        pass
    create_file_CLI(num)
