import re

pstart = re.compile(r'\d\d:\d\d:\d\d.*Start rule\d')
pend = re.compile(r'\d\d:\d\d:\d\d.*\n.*\d\d:\d\d:\d\d.*Stop rule\d')

action = [[], [], []]
hack = 3
a = ""
with open("/root/smart-data-1.3.2/logs/smartserver.log", 'r') as f:
    a = f.read()
start = pstart.findall(a)
end = pend.findall(a)
print len(start)
print len(end)
for i in start:
    print i
for i in end:
    print i
for i in range(len(start)):
    n = int(start[i][-1])
    s = start[i][0:8].split(":")
    e = end[i][0:8].split(":")
    t = [int(e[j])-int(s[j]) for j in range(3)]
    t = t[0]*3600+t[1]*60+t[2]
    action[n-1-hack].append(t)

print "-----------------------------------"
print action
