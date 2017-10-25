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

TEST_DIR = "/test/"


def random_file_path():
    return TEST_DIR + random_string()


def random_string():
    return str(uuid.uuid4())


def check_post_resp(resp):
    if resp.status_code != 201:
        raise IOError("Post fails")


def check_get_resp(resp):
    if resp.status_code != 200:
        raise IOError("Get fails")


def all_success(cmds):
    for cmd in cmds:
        try:
            if cmd is None or cmd['state'] == "FAILED":
                return False
        except Exception:
            return False
    return True


def move_cmdlet(mover_type, file_path):
    return submit_cmdlet(mover_type + " -file " + file_path)


def submit_cmdlet(cmdlet_str):
    """
    submit cmdlet then return cid
    """
    resp = requests.post(CMDLET_ROOT + "/submit", data=cmdlet_str)
    return resp.json()["body"]


def get_cmdlet(cid):
    """
    get cmdlet json with cid
    """
    resp = requests.get(CMDLET_ROOT + "/" + str(cid) + "/info")
    return resp.json()["body"]


def wait_for_cmdlet(cid, period=300):
    """
    wait at most 300 seconds for cmdlet to be done
    """
    timeout = time.time() + period
    while True:
        cmdlet = get_cmdlet(cid)
        if cmdlet['state'] == "PENDING" or cmdlet['state'] == "EXECUTING":
            time.sleep(1)
        elif cmdlet['state'] == "DONE" or cmdlet['state'] == "FAILED":
            return cmdlet
        if time.time() >= timeout:
            return None


def wait_for_cmdlets(cids, period=300):
    failed_cids = []
    while len(cids) != 0:
        cmd = wait_for_cmdlet(cids[0])
        if cmd is None or cmd['state'] == 'FAILED':
            failed_cids.append(cids[0])
        cids.pop(0)
    return failed_cids


def get_rule(rid):
    resp = requests.get(RULE_ROOT + "/" + str(rid) + "/info",
                        data=str(rid))
    return resp.json()["body"]


def list_rule():
    resp = requests.get(RULE_ROOT + "/list")
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


def list_action():
    resp = requests.get(ACTION_ROOT + "/list")
    return resp.json()["body"]


def read_file(file_path):
    cmdlet_str = "read -file " + file_path
    return submit_cmdlet(cmdlet_str)


def create_file(file_path, length=1024):
    cmdlet_str = "write -file " + file_path + " -length " + str(length)
    return submit_cmdlet(cmdlet_str)


def create_random_file(length=1024):
    """
    create a random file in /test/
    """
    file_path = TEST_DIR + random_string()
    cmdlet_str = "write -file " + \
                 file_path + " -length " + str(length)
    wait_for_cmdlet(submit_cmdlet(cmdlet_str))
    return file_path


def create_random_file_parallel(length=1024):
    """
    create a random file in /test/
    """
    return create_random_file_parallel(TEST_DIR, length)


def create_random_file_parallel(dest_path, length=1024):
    """
    create a random file in /dest_path/
    """
    file_path = dest_path + random_string()
    cmdlet_str = "write -file " + \
                 file_path + " -length " + str(length)
    return file_path, submit_cmdlet(cmdlet_str)


def delete_file(file_path, recursivly=True):
    cmdlet_str = "delete -file " + file_path
    return submit_cmdlet(cmdlet_str)


def append_file(file_path, length=1024):
    """
    append random content to file_path
    """
    cmdlet_str = "append -file " + file_path + " -length " + str(length)
    return submit_cmdlet(cmdlet_str)


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
    file_path = TEST_DIR + random_string()
    cmd_create = wait_for_cmdlet(create_file(file_path, length))
    cmd_move = wait_for_cmdlet(move_cmdlet(mover_type, file_path))
    return cmd_create, cmd_move


def move_random_file_twice(mover_type_1, mover_type_2, length):
    file_path = TEST_DIR + random_string()
    cmd_create = wait_for_cmdlet(create_file(file_path, length))
    cmd_move_1 = wait_for_cmdlet(move_cmdlet(mover_type_1, file_path))
    cmd_move_2 = wait_for_cmdlet(move_cmdlet(mover_type_2, file_path))
    return cmd_create, cmd_move_1, cmd_move_2


def move_randomly(file_path):
    """
    Randomly move blocks of a given file
    """
    index = random.randrange(len(MOVE_TYPE))
    return submit_cmdlet(MOVE_TYPE[index] + " -file " + file_path)


def continualy_move(moves, file_path):
    cmds = []
    for move in moves:
        cmds.append(wait_for_cmdlet(move_cmdlet(move, file_path)))
    return cmds


def random_move_list(length=10):
    """
    Generate a rabdin move list with given length.
    Note that neighbor moves must be different.
    """
    moves = []
    last_move = -1
    while length > 0:
        random_index = random.randrange(len(MOVE_TYPE))
        if random_index != last_move:
            last_move = random_index
            moves.append(MOVE_TYPE[random_index])
            length -= 1
    return moves


def random_move_list_totally(length=10):
    """
    Generate a rabdin move list with given length.
    """
    moves = []
    while length > 0:
        random_index = random.randrange(len(MOVE_TYPE))
        moves.append(MOVE_TYPE[random_index])
        length -= 1
    return moves


def move_random_task_list(file_size):
    """
    Generate a random file with given size, and
    generate rand a move list (nearbor move is different).
    Then, move this file continualy.
    """
    file_path = random_file_path()
    wait_for_cmdlet(create_file(file_path, file_size))
    # check_storage(file_path)
    # use a list to save the result
    # record the last task
    moves = random_move_list(random.randrange(10, 21))
    return continualy_move(moves, file_path)


def move_random_task_list_totally(file_size):
    """
    Generate a random file with given size, and
    generate rand a move list.
    Then, move this file continualy.
    """
    file_path = random_file_path()
    wait_for_cmdlet(create_file(file_path, file_size))
    # check_storage(file_path)
    # use a list to save the result
    # record the last task
    moves = random_move_list_totally(random.randrange(10, 21))
    return continualy_move(moves, file_path)
