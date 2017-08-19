import requests
import sys
import random
import time
import unittest

# Server info
BASE_URL = "http://localhost:7045"

# Restapi root
REST_ROOT = BASE_URL + "/smart/api/v1"
RULE_ROOT = REST_ROOT + "/rules"
CMDLET_ROOT = REST_ROOT + "/cmdlets"
ACTION_ROOT = REST_ROOT + "/actions"
CLUSTER_ROOT = REST_ROOT + "/cluster"
SYSTEM_ROOT = REST_ROOT + "/system"
CONF_ROOT = REST_ROOT + "/conf"
PRIMARY_ROOT = REST_ROOT + "/primary"

MOVE_TYPE = ["onessd",
             "allssd",
             "archive"]
TEST_FILES = ["/test/data_10GB",
              "/test/data_2GB",
              "/test/data_1GB",
              "/test/data_10MB",
              "/test/data_64MB"]


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
    resp = requests.get(RULE_ROOT + "/" + str(rid) + "/info",
                        data={'ruleId', str(rid)})
    return resp.json()["body"]


def submit_rule(rule_str):
    resp = requests.post(RULE_ROOT + "/add", data={'ruleText': rule_str})
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
    str = "read -file " + file_path
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

def mover_from_archive(dest_type):
    randomNum = random.randInt(1, 99999999)

    file_path_10MB = "/test/" + randomNum + "_10MB"
    file_path_64MB = "/test/" + randomNum + "_64MB"
    file_path_1GB = "/test/" + randomNum + "_1GB"
    file_path_2GB = "/test/" + randomNum + "_2GB"
    file_path_10GB = "/test/" + randomNum + "10GB"

    cid_create_10MB = create_file(file_path_10MB, 10 * 1024)
    cid_dest_10MB = submit_cmdlet(dest_type + " -file " + file_path_10MB)
    self.assertTrue(wait_for_cmdlet(cid_create_10MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_10MB)['state'] == "DONE")

    cid_create_64MB = create_file(file_path_64MB, 64 * 1024)
    cid_dest_64MB = submit_cmdlet(dest_type + " -file " + file_path_64MB)
    self.assertTrue(wait_for_cmdlet(cid_create_64MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_64MB)['state'] == "DONE")

    cid_create_1GB = create_file(file_path_1GB, 1024 * 1024)
    cid_dest_1GB = submit_cmdlet(dest_type + " -file " + file_path_1GB)
    self.assertTrue(wait_for_cmdlet(cid_create_1GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_1GB)['state'] == "DONE")

    cid_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
    cid_dest_2GB = submit_cmdlet(dest_type + " -file " + file_path_2GB)
    self.assertTrue(wait_for_cmdlet(cid_create_2GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_2GB)['state'] == "DONE")

    cid_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
    cid_dest_10GB = submit_cmdlet(dest_type + " -file " + file_path_10GB)
    self.assertTrue(wait_for_cmdlet(cid_create_10GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_10GB)['state'] == "DONE")

def test_mover_from_archive_init():
    mover_from_archive("archive")
    mover_from_archive("onessd")
    mover_from_archive("allssd")

def mover_from_onessd(dest_type):
    randomNum = random.randInt(1, 99999999)

    file_path_10MB = "/test/" + randomNum + "_10MB"
    file_path_64MB = "/test/" + randomNum + "_64MB"
    file_path_1GB = "/test/" + randomNum + "_1GB"
    file_path_2GB = "/test/" + randomNum + "_2GB"
    file_path_10GB = "/test/" + randomNum + "10GB"

    cid_create_10MB = create_file(file_path_10MB, 10 * 1024)
    cid_onessd_10MB = submit_cmdlet("onessd -file " + file_path_10MB)
    cid_dest_10MB = submit_cmdlet(dest_type + " -file " + file_path_10MB)
    self.assertTrue(wait_for_cmdlet(cid_onessd_10MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_10MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_10MB)['state'] == "DONE")

    cid_create_64MB = create_file(file_path_64MB, 64 * 1024)
    cid_onessd_64MB = submit_cmdlet("onessd -file " + file_path_64MB)
    cid_dest_64MB = submit_cmdlet(dest_type + " -file " + file_path_64MB)
    self.assertTrue(wait_for_cmdlet(cid_onessd_64MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_64MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_64MB)['state'] == "DONE")

    cid_create_1GB = create_file(file_path_1GB, 1024 * 1024)
    cid_onessd_1GB = submit_cmdlet("onessd -file " + file_path_1GB)
    cid_dest_1GB = submit_cmdlet(dest_type + " -file " + file_path_1GB)
    self.assertTrue(wait_for_cmdlet(cid_onessd_1GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_1GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_1GB)['state'] == "DONE")

    cid_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
    cid_onessd_2GB = submit_cmdlet("onessd -file " + file_path_2GB)
    cid_dest_2GB = submit_cmdlet(dest_type + " -file " + file_path_2GB)
    self.assertTrue(wait_for_cmdlet(cid_onessd_2GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_2GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_2GB)['state'] == "DONE")

    cid_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
    cid_onessd_10GB = submit_cmdlet("onessd -file " + file_path_10GB)
    cid_dest_10GB = submit_cmdlet(dest_type + " -file " + file_path_10GB)
    self.assertTrue(wait_for_cmdlet(cid_onessd_10GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_10GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_10GB)['state'] == "DONE")

def test_mover_from_onessd_init():
    mover_from_onessd("archive")
    mover_from_onessd("allssd")
    mover_from_onessd("onessd")

def mover_from_allssd(dest_type):
    randomNum = random.randInt(1, 99999999)

    file_path_10MB = "/test/" + randomNum + "_10MB"
    file_path_64MB = "/test/" + randomNum + "_64MB"
    file_path_1GB = "/test/" + randomNum + "_1GB"
    file_path_2GB = "/test/" + randomNum + "_2GB"
    file_path_10GB = "/test/" + randomNum + "10GB"

    cid_create_10MB = create_file(file_path_10MB, 10 * 1024)
    cid_allssd_10MB = submit_cmdlet("allssd -file " + file_path_10MB)
    cid_dest_10MB = submit_cmdlet(dest_type + " -file " + file_path_10MB)
    self.assertTrue(wait_for_cmdlet(cid_allssd_10MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_10MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_10MB)['state'] == "DONE")

    cid_create_64MB = create_file(file_path_64MB, 64 * 1024)
    cid_allssd_64MB = submit_cmdlet("allssd -file " + file_path_64MB)
    cid_dest_64MB = submit_cmdlet(dest_type + " -file " + file_path_64MB)
    self.assertTrue(wait_for_cmdlet(cid_allssd_64MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_64MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_64MB)['state'] == "DONE")

    cid_create_1GB = create_file(file_path_1GB, 1024 * 1024)
    cid_allssd_1GB = submit_cmdlet("allssd -file " + file_path_1GB)
    cid_dest_1GB = submit_cmdlet(dest_type + " -file " + file_path_1GB)
    self.assertTrue(wait_for_cmdlet(cid_allssd_1GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_1GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_1GB)['state'] == "DONE")

    cid_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
    cid_allssd_2GB = submit_cmdlet("allssd -file " + file_path_2GB)
    cid_dest_2GB = submit_cmdlet(dest_type + " -file " + file_path_2GB)
    self.assertTrue(wait_for_cmdlet(cid_allssd_2GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_2GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_2GB)['state'] == "DONE")

    cid_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
    cid_allssd_10GB = submit_cmdlet("allssd -file " + file_path_10GB)
    cid_dest_10GB = submit_cmdlet(dest_type + " -file " + file_path_10GB)
    self.assertTrue(wait_for_cmdlet(cid_allssd_10GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_10GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_dest_10GB)['state'] == "DONE")

def test_mover_from_allssd_init():
    mover_from_allssd("archive")
    mover_from_allssd("allssd")
    mover_from_allssd("onessd")

#the case doesn't include overwrite
def mover_while_doing_other_operation(movetype,otheroperation):
    randomNum = random.randInt(1, 99999999)

    file_path_10MB = "/test/" + randomNum + "_10MB"
    file_path_64MB = "/test/" + randomNum + "_64MB"
    file_path_1GB = "/test/" + randomNum + "_1GB"
    file_path_2GB = "/test/" + randomNum + "_2GB"
    file_path_10GB = "/test/" + randomNum + "10GB"

    cid_create_10MB = create_file(file_path_10MB, 10 * 1024)
    cid_movetype_10MB = submit_cmdlet(movetype + " -file " + file_path_10MB)
    cid_otheroperation_10MB = submit_cmdlet(otheroperation + " -file " + file_path_10MB)
    self.assertTrue(wait_for_cmdlet(cid_otheroperation_10MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_10MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_10MB)['state'] == "DONE")

    cid_create_64MB = create_file(file_path_64MB, 64 * 1024)
    cid_movetype_64MB = submit_cmdlet(movetype_type + " -file " + file_path_64MB)
    cid_otheroperation_64MB = submit_cmdlet(otheroperation + " -file " + file_path_64MB)
    self.assertTrue(wait_for_cmdlet(cid_otheroperation_64MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_64MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_64MB)['state'] == "DONE")

    cid_create_1GB = create_file(file_path_1GB, 1024 * 1024)
    cid_movetype_1GB = submit_cmdlet(movetype_type + " -file " + file_path_1GB)
    cid_otheroperation_1GB = submit_cmdlet(otheroperation + " -file " + file_path_1GB)
    self.assertTrue(wait_for_cmdlet(cid_otheroperation_1GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_1GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_1GB)['state'] == "DONE")

    cid_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
    cid_movetype_2GB = submit_cmdlet(movetype_type + " -file " + file_path_2GB)
    cid_otheroperation_2GB = submit_cmdlet(otheroperation + " -file " + file_path_2GB)
    self.assertTrue(wait_for_cmdlet(cid_otheroperation_2GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_2GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_2GB)['state'] == "DONE")

    cid_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
    cid_movetype_10GB = submit_cmdlet(movetype_type + " -file " + file_path_10GB)
    cid_otheroperation_10GB = submit_cmdlet(otheroperation_type + " -file " + file_path_10GB)
    self.assertTrue(wait_for_cmdlet(cid_otheroperation_10GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_10GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_10GB)['state'] == "DONE")

def test_mover_while_doing_other_operation_init():
    mover_while_doing_other_operation("allssd","read")
    mover_while_doing_other_operation("allssd","delete")
    mover_while_doing_other_operation("allssd", "append")

    mover_while_doing_other_operation("onessd", "read")
    mover_while_doing_other_operation("onessd", "delete")
    mover_while_doing_other_operation("onessd", "append")

    mover_while_doing_other_operation("archive", "read")
    mover_while_doing_other_operation("archive", "delete")
    mover_while_doing_other_operation("archive", "append")

def mover_while_doing_overwrite(movetype):
    randomNum = random.randInt(1, 99999999)

    file_path_10MB = "/test/" + randomNum + "_10MB"
    file_path_64MB = "/test/" + randomNum + "_64MB"
    file_path_1GB = "/test/" + randomNum + "_1GB"
    file_path_2GB = "/test/" + randomNum + "_2GB"
    file_path_10GB = "/test/" + randomNum + "10GB"

    cid_create_10MB = create_file(file_path_10MB, 10 * 1024)
    cid_movetype_10MB = submit_cmdlet(movetype + " -file " + file_path_10MB)
    cid_write_10MB = create_file(file_path_10MB, 10 * 1024)
    self.assertTrue(wait_for_cmdlet(cid_write_10MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_10MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_10MB)['state'] == "DONE")

    cid_create_64MB = create_file(file_path_64MB, 64 * 1024)
    cid_movetype_64MB = submit_cmdlet(movetype_type + " -file " + file_path_64MB)
    cid_write_64MB = create_file(file_path_64MB, 64 * 1024)
    self.assertTrue(wait_for_cmdlet(cid_write_64MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_64MB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_64MB)['state'] == "DONE")

    cid_create_1GB = create_file(file_path_1GB, 1024 * 1024)
    cid_movetype_1GB = submit_cmdlet(movetype_type + " -file " + file_path_1GB)
    cid_write_1GB = create_file(file_path_1GB, 1024 * 1024)
    self.assertTrue(wait_for_cmdlet(cid_write_1GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_1GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_1GB)['state'] == "DONE")

    cid_create_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
    cid_movetype_2GB = submit_cmdlet(movetype_type + " -file " + file_path_2GB)
    cid_write_2GB = create_file(file_path_2GB, 2 * 1024 * 1024)
    self.assertTrue(wait_for_cmdlet(cid_write_2GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_2GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_2GB)['state'] == "DONE")

    cid_create_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
    cid_movetype_10GB = submit_cmdlet(movetype_type + " -file " + file_path_10GB)
    cid_write_10GB = create_file(file_path_10GB, 10 * 1024 * 1024)
    self.assertTrue(wait_for_cmdlet(cid_write_10GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_create_10GB)['state'] == "DONE")
    self.assertTrue(wait_for_cmdlet(cid_movetype_10GB)['state'] == "DONE")

def test_mover_while_doing_overwrite_init():
    mover_while_doing_overwrite("allssd")
    mover_while_doing_overwrite("onessd")
    mover_while_doing_overwrite("archive")


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
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 10 GB file
        file_path = TEST_FILES[0]
        cid_move = submit_cmdlet("allssd -file " + file_path)
        check_storage(file_path)
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid=cid_read)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid=cid_move)['state'] == "DONE")

    def test_mover_write(self):
        # cid_create = create_file("/testFile")
        # print check_storage("/testFile")
        # Test with 10 GB file
        file_path = TEST_FILES[0]
        cid_move = submit_cmdlet("allssd -file " + file_path)
        check_storage(file_path)
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid=cid_read)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid=cid_move)['state'] == "DONE")

        file_path = TEST_FILES[1]
        cid_move = submit_cmdlet("onessd -file " + file_path)
        check_storage(file_path)
        # read the file
        cid_read = read_file(file_path)
        # check the statement of read
        self.assertTrue(wait_for_cmdlet(cid=cid_read)['state'] == "DONE")
        self.assertTrue(wait_for_cmdlet(cid=cid_move)['state'] == "DONE")

    def test_mover_stress(self):
        # TODO launch 100000 move actions
        pass

    def test_rule_hot(self):
        # Submit rule
        rule_str = "file : path matches \"/test/*\" and accessCount(1m) > 1 | allssd "
        rid = submit_rule(rule_str)
        start_rule(rid)
        file_path = TEST_FILES[random.randrange(len(TEST_FILES))]
        # Activate rule
        # Submit read action to trigger rule
        # Read twice
        cid_r1 = read_file(file_path)
        cid_r2 = read_file(file_path)
        cid_r3 = read_file(file_path)
        wait_for_cmdlet(cid_r1)
        wait_for_cmdlet(cid_r2)
        wait_for_cmdlet(cid_r3)
        time.sleep(15)
        # Statue check
        rule = get_rule(rid)
        self.assertTrue(rule['numCmdsGen'] > 0)
        delete_rule(rid)

    def test_rule_cold(self):
        # Submit rule
        rule_str = "file : path matches \"/test/*\" and age > 4s | archive "
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
