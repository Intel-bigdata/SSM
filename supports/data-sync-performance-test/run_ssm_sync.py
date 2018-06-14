import sys
import time
from util import *

case = sys.argv[1]
round = sys.argv[2]
dest_cluster = sys.argv[3]

rid = submit_rule("file: path matches \"/" + case + "/*\" | sync -dest " + dest_cluster + "/" + case)
start_rule(rid)

start = False
while True:
    runningProgress = get_sync_info_by_rid(rid)["runningProgress"]
    if start:
        if runningProgress == 0:
            break
    else:
        if runningProgress > 0:
            start = True
    time.sleep(1)
wait_for_cmdlets(get_cids_of_rule(rid))
stop_rule(rid)