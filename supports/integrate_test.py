import requests
import sys
import random
import time

# Server info
BASE_URL = "http://localhost:8080"

# Restapi root
REST_ROOT = BASE_URL + "/smart/api/v1"
RULE_ROOT = REST_ROOT + "/rules"
CMDLET_ROOT = REST_ROOT + "/cmdlets"
ACTION_ROOT = REST_ROOT + "/actions"
CLUSTER_ROOT = REST_ROOT + "/cluster"
SYSTEM_ROOT = REST_ROOT + "/system"
CONF_ROOT = REST_ROOT + "/conf"
PRIMARY_ROOT = REST_ROOT + "/primary"


MOVE_TYPE = ["onessd", "allssd", "archive"]
TEST_FILES = ["/test/data_1GB", "/test/data_10MB", "/test/data_64MB"]


def check_post_resp():
    if resp.status_code != 201:
        raise ApiError("Post fails")


def check_get_resp():
    if res.status_code != 200:
        raise ApiError("Get fails")


def submit_cmdlet(cmd_str):
    resp = requests.post(CMDLET_ROOT + "/submit", data=cmd_str)
    return resp.json()["body"]


def get_cmdlet(cid):
    resp = requests.get(CMDLET_ROOT + "/" + str(cid) + "/info")
    return resp.json()["body"]


def wait_for_cmdlet(cid, period=40):
    # Set 40 Seconds
    timeout = time.time() + period
    while True:
        cmdlet = get_cmdlet(cid)
        if cmdlet['state'] == "PENDING" or cmdlet['state'] == "EXECUTING":
            time.sleep(1)
        elif cmdlet['state'] == "DONE" or cmdlet['state'] == "FAILED":
            return cmdlet
        if time.time() >= timeout:
            return null


def get_rule(rid):
    resp = requests.get(RULE_ROOT + "/" + str(cid) + "/info")
    return resp.json()["body"]


def submit_rule(rid):
    pass


def get_action(aid):
    resp = requests.get(ACTION_ROOT + "/" + str(cid) + "/info")
    return resp.json()["body"]


def read_file(file_path):
    pass


def create_file(file_path, length=1024):
    pass


def delete_file(file_path, recursivly=True):
    pass


def random_move(file_path):
    index = random.randrange(len(MOVE_TYPE))
    resp = requests.post(CMDLET_ROOT + "/submit", data=MOVE_TYPE[index] + " -file  " + file_path)
    return resp.json()["body"]


def check_storage(file_path):
    resp = requests.post(CMDLET_ROOT + "/submit", data="checkstorage -file  " + file_path)
    cid = resp.json()["body"]
    cmdlet = wait_for_cmdlet(cid)
    aid = cmdlet['aids']
    return get_action(aid[0])


def test_mover():
    # Case 1
    # Checkstorage
    # random move sequentially
    # count = len(MOVE_TYPE)
    # while count > 0:
    #     print wait_for_cmdlet(random_move("/test/data_1GB"))
    #     print wait_for_cmdlet(random_move("/test/data_10MB"))
    #     print wait_for_cmdlet(random_move("/test/data_64MB"))
    #     count -= 1

    # Parallel random move
    queue = []
    count = len(MOVE_TYPE)
    while count > 0:
        # Submit cmdlets
        for index in range(len(TEST_FILES)):
            queue.add(random_move())
        # check status
        while len(queue) != 0:
            wait_for_cmdlet(queue[0])
            queue.pop(0)
        count -= 1
    # Case 2
    # Case 3


def test_mover_protection():
    pass


def test_action():
    pass


def test_rule():
    pass


if __name__ == '__main__':
    test_mover()