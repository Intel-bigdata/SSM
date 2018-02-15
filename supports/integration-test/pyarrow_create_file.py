import sys
from multiprocessing import Pool, Queue, Value, Lock
import pyarrow as pa
import time

from util import *

HOST = "localhost"
PORT = 9000
MAX_TASK = cpu_count()
FILE_NUM = 10000
queue = Queue()
finish = Value('i', 0)
lock = Lock()


def create_file_pyarrow():
    fs = pa.hdfs.connect(HOST, PORT)
    while True:
        lock.acquire()
        file_name = queue.get()
        if file_name is None:
            lock.release()
            break
        finish.value = finish.value-1
        lock.release()
        with fs.open(file_name, "wb") as f:
            f.write("")


def master(total, DIR_NAMES):
    for DIR_NAME in DIR_NAMES:
        for j in range(FILE_NUM):
            queue.put(DIR_NAME + str(j))
    for _ in range(MAX_TASK-1):
        queue.put(None)
    while finish.value > 0:
        percent = "%2d%%" % int(((total-finish.value)/float(total))*100)
        sys.stdout.write(percent+"\b\b\b")
        sys.stdout.flush()
        time.sleep(1.1)
    sys.stdout.write("100%")
    sys.stdout.flush()
    print


def main(num):
    stime = time.time()
    finish.value = total = num*FILE_NUM
    DIR_NAMES = [TEST_DIR + random_string() + '/' for _ in range(num)]
    p = Pool()
    print "creating files"
    p.apply_async(master, args=(total, DIR_NAMES,))
    for _ in range(MAX_TASK-1):
        p.apply_async(create_file_pyarrow)
    p.close()
    p.join()
    etime = time.time()
    print "checking files"
    fs = pa.hdfs.connect(HOST, PORT)
    for i in range(num):
        DIR_NAME = DIR_NAMES[i]
        if len(fs.ls(DIR_NAME)) != FILE_NUM:
            print "ERROR!"
            sys.exit()
        else:
            info = "%d/%d" % (i, num)
            sys.stdout.write(info+'\b'*len(info))
            sys.stdout.flush()
    print "create %d*10K files in %fs" % (num, etime-stime)

if __name__ == '__main__':
    try:
        HOST = sys.argv[1]
        PORT = int(sys.argv[2])
        NUM = int(sys.argv[3])
    except:
        print "Usage: python pyarrow_create_file.py [HOST] [PORT] [num]"
        sys.exit()
    main(NUM)
