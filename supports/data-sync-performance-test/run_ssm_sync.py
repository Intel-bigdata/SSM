import sys
import time
from util import *

size = sys.argv[1]
num = sys.argv[2]
case = size + "_" + num
log = sys.argv[3]
dest_cluster = sys.argv[4]

rid = submit_rule("file: path matches \"/" + case + "/*\" | sync -dest " + dest_cluster + "/" + case)
start_rule(rid)
start_time = time.time()
while True:
    rule = get_rule(rid)
    if rule['numCmdsGen'] == int(num):
        break
    time.sleep(1)
failed_cids = wait_for_cmdlets(get_cids_of_rule(rid))
if len(failed_cids) != 0:
    print "Not all sync actions succeed!"
stop_rule(rid)
end_time = time.time()
# append result to log file
f = open(log, 'a')
f.write(str(int(end_time - start_time)) + "s" + "  ")
f.close()
