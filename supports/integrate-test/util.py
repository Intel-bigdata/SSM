import requests
import random
import time
import uuid

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
              "/test/data_64MB",
              "/test/data_10MB"]


def random_string():
    return uuid.uuid4()


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


def random_move_test_file(file_path):
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


def move_random_file(mover_type, length):
    file_path = "/test/" + random_string
    cid_create = create_file(file_path, length)
    cid_dest = submit_cmdlet(mover_type + " - file " + file_path)
    return cid_create, cid_dest
