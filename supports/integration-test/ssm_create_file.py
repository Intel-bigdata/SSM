import sys
from util import *
from threading import Thread


def test_create_100M_0KB_thread(max_number):
    """ submit SSM action througth Restful API in parallel
    """
    cids = []
    dir_name = TEST_DIR + random_string()
    for j in range(max_number):
        # each has 200K files
        cid = create_file(dir_name + "/" + str(j), 0)
        cids.append(cid)
    wait_for_cmdlets(cids)


if __name__ == '__main__':
    num = 20
    try:
        num = int(sys.argv[1])
    except ValueError:
        print "Usage: python dfsio_create_file [num]"
    except IndexError:
        pass
    max_number = 200000
    for i in range(num):
        t = Thread(target=test_create_100M_0KB_thread, args=(max_number,))
        t.start()
