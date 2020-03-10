import sys
import time
from util import *

size = sys.argv[1]
num = sys.argv[2]

#The data dir is named by case. Please see prepare.sh
case = size + "_" + num
log = sys.argv[3]
#Either "ec" or "unec"
action = sys.argv[4]

if action == "compressDFSIO":
    rid = submit_rule("file: path matches \"/" + case + "/*\"| compress -compressImpl Zlib")
elif action == "compressTerasort":
    #need config map num and hibench.slave/master to ensure the rule will be stopped in hibench.conf
    rid = submit_rule("file: path matches \"/HiBench/Terasort/Input/*\"| compress -compressImpl Zlib")

start_rule(rid)
start_time = time.time()
rule = get_rule(rid)
last_checked = rule['numChecked']
last_cmdsgen = rule['numCmdsGen']
time.sleep(.1)

#Check whether all expected cmdlets have been generated.
#The overall cmdlets' num should equal to the test files' num,
#if not, wait for more cmdlets to be generated.
cids = get_cids_of_rule(rid)
while len(cids) < int(num):
  time.sleep(.1)
  rule = get_rule(rid)
  cids = get_cids_of_rule(rid)
time.sleep(.1)
cids = get_cids_of_rule(rid)

wait_cmdlets(cids)

end_time = time.time()
stop_rule(rid)
# append result to log file
f = open(log, 'a')
f.write(str(int(end_time - start_time)) + "s" + "  " + '\n')
f.close()
