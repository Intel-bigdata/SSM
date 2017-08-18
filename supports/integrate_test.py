import requests
import sys
import random
import time
import unittest

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


def check_post_resp(resp):
    if resp.status_code != 201:
        raise IOError("Post fails")


def check_get_resp(resp):
    if resp.status_code != 200:
        raise IOError("Get fails")


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
            return None


def get_rule(rid):
    resp = requests.get(RULE_ROOT + "/" + str(rid) + "/info")
    return resp.json()["body"]


def submit_rule(rid_str):
    resp = requests.post(RULE_ROOT + "/submit", data=rid_str)
    return resp.json()["body"]


def get_action(aid):
    resp = requests.get(ACTION_ROOT + "/" + str(aid) + "/info")
    return resp.json()["body"]


def read_file(file_path):
    str = "read -file" + file_path
    submit_cmdlet(str)


def create_file(file_path, length=1024):
    str = "write -file " + file_path + " -length " + length
    submit_cmdlet(str)


def delete_file(file_path, recursivly=True):
    str = "delete -file " + file_path
    submit_cmdlet(str)


def append_to_file(file_path, length=1024):
    str = "append -file " + file_path + " -length " + length
    submit_cmdlet(str)


def random_move(file_path):
    index = random.randrange(len(MOVE_TYPE))
    resp = requests.post(CMDLET_ROOT + "/submit",
                         data=MOVE_TYPE[index] + " -file  " + file_path)
    return resp.json()["body"]


def check_storage(file_path):
    resp = requests.post(CMDLET_ROOT + "/submit",
                         data="checkstorage -file  " + file_path)
    cid = resp.json()["body"]
    cmdlet = wait_for_cmdlet(cid)
    aid = cmdlet['aids']
    return get_action(aid[0])


class IntegrateTest(unittest.TestCase):
    def test_mover_sequentially(self):
        # Case 1
        # random move sequentially
        queue = []
        count = len(MOVE_TYPE)
        while count > 0:
            # Submit cmdlets
            for index in range(len(TEST_FILES)):
                cmdlet = wait_for_cmdlet(random_move(TEST_FILES[index]))
                if cmdlet is None:
                    queue.append(cmdlet['cid'])
            count -= 1
        self.assertTrue(len(queue) == 0)

    def test_mover_parallel(self):
        # Case 2
        # Parallel random move
        queue = []
        count = len(MOVE_TYPE)
        while count > 0:
            # Submit cmdlets
            for index in range(len(TEST_FILES)):
                queue.add(random_move(TEST_FILES[index]))
            # check status
            while len(queue) != 0:
                print wait_for_cmdlet(queue[0])
                queue.pop(0)
            count -= 1
        self.assertTrue(len(queue) == 0)

    def test_mover_read(self):
        # TODO Read files while moving
        pass

    def test_mover_append(self):
        # TODO Append files while moving
        pass

    def test_rule_hot(self):
        # Submit rule
        # Activate rule
        # Submit read action to trigger rule
        # Statue check
        pass

    def test_rule_cold(self):
        # Submit rule
        # Activate rule
        # wait to trigger rule
        # Statue check
        pass

    def test_rule_delete(self):
        pass

    def test_rule(self):
        pass


if __name__ == '__main__':
    unittest.main()
