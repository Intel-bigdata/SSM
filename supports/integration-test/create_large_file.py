import sys
from multiprocessing import Pool, Queue, Value, Lock
import pyarrow as pa
import time
import random

from util import *

HOST = "localhost"
PORT = 9000
MAX_TASK = cpu_count()
FILE_NUM = 10000
FILE_SIZE = 10
queue = Queue()
finish = Value('i', 0)
lock = Lock()
letter = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'


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
            f.write(FILE_SIZE*1024*random.choice(letter))


def master(total, DIR_NAME):
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


def main():
    finish.value = total = FILE_NUM
    p = Pool()
    print "creating files"
    p.apply_async(master, args=(total, DIR_NAME,))
    for _ in range(MAX_TASK-1):
        p.apply_async(create_file_pyarrow)
    p.close()
    p.join()

if __name__ == '__main__':
    try:
        HOST = sys.argv[1]
        PORT = int(sys.argv[2])
        DIR_NAME = sys.argv[3]+"/"
        FILE_SIZE = int(sys.argv[4])
        FILE_NUM = int(sys.argv[5])
    except:
        print "Usage: python create_large_file.py [HOST]"\
            "[PORT] [file path] [file size(KB)] [file number]"
        print "example: python create_large_file.py"\
            "sr519 9000 /10MB_10000 10240 10000"
        sys.exit()
    main()
