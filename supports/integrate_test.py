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
TEST_FILES = ["/test/data_10GB", "/test/data_1GB", "/test/data_10MB", "/test/data_64MB"]


def check_post_resp(resp):
    if resp.status_code != 201:
        raise IOError("Post fails")


def check_get_resp(resp):
    if resp.status_code != 200:
        raise IOError("Get fails")


def submit_cmdlet(cmd_str):
    """
    submit cmdlet then return cid
    """
    resp = requests.post(CMDLET_ROOT + "/submit", data=cmd_str)
    return resp.json()["body"]


def get_cmdlet(cid):
    """
    get cmdlet json with cid
    """
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


def submit_rule(rule_str):
    resp = requests.post(RULE_ROOT + "/submit", data=rule_str)
    return resp.json()["body"]


def delete_rule(rid):
    requests.post(RULE_ROOT + "/" + str(rid) + "/delete")


def start_rule(rid):
    requests.post(RULE_ROOT + "/" + str(rid) + "/start")


def stop_rule(rid):
    requests.post(RULE_ROOT + "/" + str(rid) + "/stop")


def get_action(aid):
    resp = requests.get(ACTION_ROOT + "/" + str(aid) + "/info")
    return resp.json()["body"]


def read_file(file_path):
    str = "read -file" + file_path
    return submit_cmdlet(str)


def create_file(file_path, length=1024):
    str = "write -file " + file_path + " -length " + length
    return submit_cmdlet(str)


def delete_file(file_path, recursivly=True):
    str = "delete -file " + file_path
    return submit_cmdlet(str)


def append_to_file(file_path, length=1024):
    str = "append -file " + file_path + " -length " + length
    return submit_cmdlet(str)


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
                print cmdlet
                wait_for_cmdlet(check_storage(TEST_FILES[index]))
                if cmdlet is None or cmdlet['state'] == "FAILED":
                    queue.append(cmdlet['cid'])
            count -= 1
        print "Failed cids = {}".format(queue)
        self.assertTrue(len(queue) == 0)

    def test_mover_parallel(self):
        # Case 2
        # Parallel random move
        queue = []
        count = len(MOVE_TYPE)
        while count > 0:
            # Submit cmdlets
            for index in range(len(TEST_FILES)):
                queue.append(random_move(TEST_FILES[index]))
            # check status
            while len(queue) != 0:
                print wait_for_cmdlet(queue[0])
                queue.pop(0)
            count -= 1
        print "Failed cids = {}".format(queue)
        self.assertTrue(len(queue) == 0)

    def test_mover_read(self):
        cid_create = create_file("/testFile")
        print check_storage("/testFile")
        cid_move = submit_cmdlet("allssd -file /testFile")
        # read the file
        cid_read = read_file("/testFile")
        #check the statement of read
        self.assertTrue(wait_for_cmdlet(cid=cid_read) == "DONE")
        self.assertTrue(wait_for_cmdlet(cid=cid_move) == "DONE")
        self.assertTrue(wait_for_cmdlet(cid=cid_create) == "DONE")


    def test_mover_append(self):
        cid_create = create_file("/testFile")
        print check_storage("/testFile")
        cid_move = submit_cmdlet("allssd -file /testFile")
        # read the file
        cid_append = read_file("/testFile")
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid=cid_append) == "DONE")
        self.assertTrue(wait_for_cmdlet(cid=cid_move) == "DONE")
        self.assertTrue(wait_for_cmdlet(cid=cid_create) == "DONE")

    def test_mover_stress(self):
        # TODO launch 100000 move actions
        pass

    def test_rule_hot(self):
        # Submit rule
        rule_str = "file : path matches \"/test/*\" and accessCount(40s) > 1 | allssd"
        rid = submit_rule(rule_str)
        start_rule(rid)
        file_path = TEST_FILES[random.randrange(len(TEST_FILES))]
        # Activate rule
        # Submit read action to trigger rule
        # Read twice
        wait_for_cmdlet(read_file(file_path))
        wait_for_cmdlet(read_file(file_path))
        # Statue check
        rule = get_rule(rid)
        self.assertTrue(rule['numCmdsGen'] > 0)
        delete_rule(rid)

    def test_rule_cold(self):
        # Submit rule
        rule_str = "file : path matches \"/testArchive/*\" and age > 4s | archive "
        rid = submit_rule(rule_str)
        start_rule(rid)
        file_path = TEST_FILES[random.randrange(len(TEST_FILES))]
        # Activate rule
        # wait to trigger rule
        # Read twice
        wait_for_cmdlet(read_file(file_path))
        time.sleep(5)
        # Statue check
        rule = get_rule(rid)
        self.assertTrue(rule['numCmdsGen'] > 0)
        delete_rule(rid)

    def test_rule_delete(self):
        pass

    def test_rule_stress(self):
        # Add 100000 different rules
        pass


if __name__ == '__main__':
    unittest.main()
