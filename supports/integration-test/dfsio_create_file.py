import sys
from util import *


def create_file_DFSIO(num):
    """
    Please use this script in namenode
    Each time create 10K * 2 files (10K in io_data and 10K in io_control).
    Then, move these data to TEST_DIR.
    """
    dfsio_cmd = "hadoop jar /usr/lib/hadoop-mapreduce/hadoop-" + \
        "mapreduce-client-jobclient-*-tests.jar TestDFSIO " + \
        "-write -nrFiles 10000 -fileSize 0KB"
    for i in range(num):
        subprocess.call(dfsio_cmd)
        subprocess.call("hdfs dfs -mv /benchmarks/TestDFSIO/io_control " +
                        TEST_DIR + str(i) + "_control")
        subprocess.call("hdfs dfs -mv /benchmarks/TestDFSIO/io_data " +
                        TEST_DIR + str(i) + "_data")


if __name__ == '__main__':
    num = 50
    try:
        num = int(sys.argv[1])
    except ValueError:
        print "Usage: python dfsio_create_file [num]"
    except IndexError:
        pass
    create_file_DFSIO(num)
