import sys
import time
from util import *


size = sys.argv[1]
num = sys.argv[2]
case = size + "_" + num
log = sys.argv[3]
action = sys.argv[4]

if action == "ec":
    #rid = submit_rule("file: every 30min | path matches \"/" + case + "/*\" | ec -policy RS-6-3-1024k")
    rid = submit_rule("file: path matches \"/" + case + "/*\" | ec -policy RS-6-3-1024k")
elif action == "unec":
    # rid = submit_rule("file: every 30min | path matches \"/" + case + "/*\" | unec")
    rid = submit_rule("file: path matches \"/" + case + "/*\" | unec")

start_rule(rid)
start_time = time.time()

rule = get_rule(rid)
last_checked = rule['numChecked']
last_cmdsgen = rule['numCmdsGen']

time.sleep(.1)
rule = get_rule(rid)
while not ((rule['numChecked'] > last_checked) and (rule['numCmdsGen'] == last_cmdsgen)):
  time.sleep(.1)
  rule = get_rule(rid)
  last_checked = rule['numChecked']
  last_cmdsgen = rule['numCmdsGen']
  time.sleep(.1)
  rule = get_rule(rid)

cids = get_cids_of_rule(rid)

#while True:
#    rule = get_rule(rid)
#    print(rule)
#    if rule['numCmdsGen'] == int(num):
#        break
#    time.sleep(1)
failed_cids = wait_for_cmdlets(cids)
if len(failed_cids) != 0:
    print "Not all ec actions succeed!"
end_time = time.time()
stop_rule(rid)
# append result to log file
f = open(log, 'a')
f.write(str(int(end_time - start_time)) + "s" + "  " + '\n')
f.close()
