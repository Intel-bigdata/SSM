import sys
from multiprocessing import Pool
import pyarrow as pa
import time

from util import *

DIR_NAME = TEST_DIR + random_string() + '/'


def create_file_pyarrow(ip, port, start, end):
    fs = pa.hdfs.connect(ip, port)
    fs.mkdir(TEST_DIR+DIR_NAME)
    for i in range(start, end):
        with fs.open(DIR_NAME+str(i), "wb") as f:
            f.write("")


def main(ip, port, num):
    stime = time.time()
    p = Pool()
    max_task = cpu_count()
    length = num/max_task
    for i in range(max_task):
        start = i*length
        end = start+length
        if i == max_task-1:
            end = num
        p.apply_async(create_file_pyarrow, args=(ip, port, start, end,))
    p.close()
    p.join()
    etime = time.time()
    fs = pa.hdfs.connect(ip, port)
    print "%fs create %d files" % (etime-stime, len(fs.ls(DIR_NAME)))

if __name__ == '__main__':
    ip = "localhost"
    port = 9000
    num = 10000
    try:
        ip = sys.argv[1]
        port = int(sys.argv[2])
        num = int(sys.argv[3])
    except:
        print "Usage: python pyarrow_create_file.py [ip] [port] [num]"
        sys.exit()
    main(ip, port, num)
