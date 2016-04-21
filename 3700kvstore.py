#!/usr/bin/env python

import sys, socket, select, time, json, random, datetime, collections

# Your ID number
my_id = sys.argv[1]

# The ID numbers of all the other replicas
replica_ids = sys.argv[2:]

# Connect to the network. All messages to/from other replicas and clients will
# occur over this socket
sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
sock.connect(my_id)

# GLOBAL VARS #
prints = True
commit_log = []
temp_log = []
leader = 'FFFF'
# the current term of leadership
term = 0
# can be follower, candidate or leader
state = 'follower'
# follower vars
elect_timer = time.time()
voted = False
# candidate vars
votes = 1
# leader vars
heartbeat = time.time()


def become_cand():
    global state, votes, leader, term, elect_timer
    state = 'candidate'
    votes = 1
    term += 1
    elect_timer = time.time()
    leader = 'FFFF'
    out = {'src': my_id, 'dst': 'FFFF', 'leader': 'FFFF', 'type': 'votereq', 'term': term, 'MID': msg['MID']}
    norm_send(sock, out)

def become_follower(leader_id):
    global state, voted, leader, elect_timer
    elect_timer = time.time()
    state = 'follower'
    voted = False
    leader = leader_id

def become_leader():
    state = 'leader'
    leader = my_id
    out = {'src': my_id, 'dst': 'FFFF', 'leader': 'FFFF', 'type': 'append', 'data': [], 'term': term, 'MID': msg['MID']}
    norm_send(sock, out)



def log(msg):
    if prints == True:
        print '{}: {}'.format(datetime.datetime.now(), msg)


def heartbeat_update():
    global heartbeat
    heartbeat = time.time()


def send_as_leader(socket, msg):
    heartbeat_update()
    socket.send(json.dumps(msg))

def norm_send(socket, message):
    socket.send(json.dumps(message))

def check_socket(socket):
    ready = select.select([socket], [], [], 0)[0]
    if socket in ready:
        msg_raw = socket.recv(32768)
        return msg_raw
    else:
        return ''

def request_update(new_term):
    global leader, term
    if commit_log:
        id = commit_log[-1]['id']
    else:
        id = -1
    out = {'src': my_id, 'dst': leader, 'leader': leader, 'type': 'update', 'id': id, 'term': term}
    term = new_term
    norm_send(sock, out)

def send_update(id, source):
    global leader, term, commit_log
    slice = 0
    for x in xrange(len(commit_log)):
        if commit_log[x]['id'] == id:
            slice = x
            break
    data = commit_log[slice:]
    out = {'src': my_id, 'dst': source, 'leader': leader, 'type': 'update', 'data': data, 'term': term}
    norm_send(sock, out)

log(('Replica {} starting up').format(my_id))

while True:


    # check for new messages
    msg_raw = check_socket(sock)
    # if there are any new messsages



    if state == 'follower':
        if len(msg_raw) > 0:
            msg = json.loads(msg_raw)
            # save the sender's id
            source = msg['src']
            # save the type of message received
            msg_type = msg['type']
            # heartbeat
            if msg_type == 'append':
                become_follower(source)
                temp_log.append(msg['data'])
                out = {"src": my_id, "dst": source, "leader": leader, "type": "ack", 'term': term}
                norm_send(sock, out)
            # redirect
            elif msg_type == 'put' or msg_type == 'get':
                out = {"src": my_id, "dst": source, "leader": leader, "type": "fail", "MID": msg['mid']}
                norm_send(sock, out)
        # heartbeat not heard in a while
        if time.time() - elect_timer > random.randint(150, 300):
            become_cand()
            continue

    if state == 'candidate':
        if len(msg_raw) > 0:
            msg = json.loads(msg_raw)
            # save the sender's id
            source = msg['src']
            # save the type of message received
            msg_type = msg['type']
            if msg_type == 'append':
                become_follower(source)
                temp_log.append(msg['data'])
                out = {"src": my_id, "dst": source, "leader": leader, "type": "ack", 'term': term}
                norm_send(sock, out)
                continue
            # redirect until election ends
            elif msg_type == 'put' or msg_type == 'get':
                out = {"src": my_id, "dst": source, "leader": 'FFFF', "type": "fail", "MID": msg['mid']}
                norm_send(sock, out)
            # getting votes
            elif msg_type == 'vote':
                votes += 1
                if votes > (len(replica_ids) / 2):
                    become_leader()
                    continue
        # heartbeat not heard in a while
        if time.time() - elect_timer > random.randint(150, 300):
            become_cand()
            continue

    if state == 'leader':
        if len(msg_raw) > 0:
            msg = json.loads(msg_raw)
            # save the sender's id
            source = msg['src']
            # save the type of message received
            msg_type = msg['type']
            if msg_type == 'append' and msg['term'] > term:
                become_follower(source)
                request_update(msg['term'])
                continue
            elif msg_type == 'update':
                send_update(msg['term'], source)
            elif msg_type == 'get':

            elif msg_type == 'put':


        # heartbeat not heard in a while
        if time.time() - elect_timer > random.randint(150, 300):
            become_cand()
            continue





# TODO LIST
# fix leader transfer issues, gate sending abilities to leaders only
# maybe we should send the queue to replicas every second or so?
# during elections, exchange log numbers to ensure the leader is the most current
