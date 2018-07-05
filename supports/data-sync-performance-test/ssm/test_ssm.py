import sys
import time
from util import *

filename = sys.argv[1]
times = sys.argv[2]
id_dict = {"10KB_10000": 1, "1MB_10000": 2, "100MB_1000": 3}
hack = 3
id = id_dict[filename]

start_rule(id+hack)

start = False
while True:
    runningProgress = list_sync()[id-1]["runningProgress"]
    if start:
        if runningProgress == 0:
            break
    else:
        if runningProgress > 0:
            start = True
    time.sleep(1)
time.sleep(3)
stop_rule(id+hack)
