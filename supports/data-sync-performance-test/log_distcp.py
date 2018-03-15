import re
import os
ts = re.compile(r'\d\d:\d\d:\d\d')


def get_time(name):
    a = ""
    with open(name,'r') as f:
        a = f.read()
    t = ts.findall(a)

    start = t[0]
    end = t[-1]
    s = start[0:8].split(":")
    e = end[0:8].split(":")
    t = [int(e[j])-int(s[j]) for j in range(3)]
    t = t[0]*3600+t[1]*60+t[2]
    return t

for m in [30,60,90]:
    for name in ["10KB_10000","1MB_10000","100MB_1000"]:
        print "%s-%d"%(name,m),
        n = 0
        total = 0
        for time in range(1,6):
            filename = "results/%s-%d-%d.log"%(name,time,m)
            if os.path.exists(filename):
                t = get_time(filename)
                print t,
                total+=t
                n+=1
        if n==0:
            continue
        print
        print total//n
