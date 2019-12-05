import sys
import time
from util import *

size = sys.argv[1]
num = sys.argv[2]
# The data dir is named by case. Please see prepare.sh.
case = size + "_" + num
log = sys.argv[3]
# Either "ec" or "unec" is acceptable.
action = sys.argv[4]

# We use a large time interval in the test rule to avoid creating too many cmdlets.
# Thus, in the test period, each file is only assigned a cmdlet which executes the ec/unec task.
if action == "ec":
    rid = submit_rule("file: every 500min | path matches \"/" + case + "/*\" | ec -policy RS-6-3-1024k")
elif action == "unec":
    rid = submit_rule("file: every 500min | path matches \"/" + case + "/*\" | unec")

start_rule(rid)
start_time = time.time()
rule = get_rule(rid)
time.sleep(.1)

# Check whether all expected cmdlets have been generated.
# The overall cmdlets' num should equal to the test files' num,
# if not, wait for more cmdlets to be generated.
cids = get_cids_of_rule(rid)   # Get all generated cmdlets' IDs.
while len(cids) < int(num):
  time.sleep(.1)
  rule = get_rule(rid)
  cids = get_cids_of_rule(rid)

time.sleep(.1)
cids = get_cids_of_rule(rid)
# Be blocked here till all cmdlets are finished.
wait_cmdlets(cids)

end_time = time.time()
stop_rule(rid)
# Append the timing result to log file
f = open(log, 'a')
f.write(str(int(end_time - start_time)) + "s" + "  " + '\n')
f.close()
