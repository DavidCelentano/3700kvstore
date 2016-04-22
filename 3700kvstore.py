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
temp_log = collections.deque()
data = {}
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
super_keys = collections.deque()
waiting_clients = collections.deque()
super_waiting_clients = collections.deque()
super_temp_log = collections.deque()
temp_keys = collections.deque()
acks = 1


def become_cand():
    global state, votes, leader, term, elect_timer
    state = 'candidate'
    votes = 1
    term += 1
    elect_timer = time.time()
    leader = 'FFFF'
    out = {'src': my_id, 'dst': 'FFFF', 'leader': 'FFFF', 'type': 'votereq', 'term': term}
    norm_send(sock, out)
    log('became candidate')

def become_follower(leader_id):
    global state, voted, leader, elect_timer
    elect_timer = time.time()
    state = 'follower'
    voted = False
    leader = leader_id

def become_leader():
    global state, leader, acks, send_time
    state = 'leader'
    leader = my_id
    out = {'src': my_id, 'dst': 'FFFF', 'leader': 'FFFF', 'type': 'append', 'data': [], 'term': term}
    norm_send(sock, out)
    log('became leader')
    acks = 1
    heartbeat_update()
    send_time = time.time()



def log(msg):
    if prints == True:
        print '{} {} {}: {}'.format(datetime.datetime.now(), state, my_id, msg)


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
            slice = x + 1
            break
    data = commit_log[slice:]
    out = {'src': my_id, 'dst': source, 'leader': leader, 'type': 'update', 'data': data, 'term': term}
    norm_send(sock, out)

def vote(cand):
    global leader, term
    out = {'src': my_id, 'dst': cand, 'leader': leader, 'type': 'vote', 'term': term}
    norm_send(sock, out)
    log('voted for {}'.format(cand))
    term += 1

def send_append():
    global temp_log, leader
    if temp_log:
        log('sent append out with temp {}'.format(len(temp_log)))
    else:
        log('sent heartbeat out')
    out = {'src': my_id, 'dst': 'FFFF', 'leader': leader, 'type': 'append', 'data': list(temp_log), 'term': term}
    norm_send(sock, out)
    heartbeat_update()

def commit_temp():
    global leader, temp_log, commit_log, waiting_clients, acks
    commit_log += temp_log
    for pair in temp_log:
        data[pair['key']] = pair['value']
    for client in waiting_clients:
        out = {"src": my_id, "dst": client[0], "leader": leader, "type": "ok", "MID": client[1]}
        norm_send(sock, out)
        log('confirmed PUT from {}'.format(client[0]))
    temp_log = collections.deque()
    waiting_clients = collections.deque()
    temp_keys = collections.deque()
    acks = 0

def replicas_commit():
    out = {"src": my_id, "dst": 'FFFF', "leader": leader, "type": "commit"}
    norm_send(sock, out)

def append_react(source, data):
    global leader, term, temp_log
    become_follower(source)
    temp_log = collections.deque(data)
    out = {"src": my_id, "dst": source, "leader": leader, "type": "ack",
           'new': len(data), 'term': term}
    norm_send(sock, out)
    log('received append')

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
                if msg['term'] > term:
                    log('need update')
                    request_update(msg['term'])
                else:
                    append_react(source, msg['data'])
                continue
            # commit
            elif msg_type == 'commit':
                for commit in temp_log:
                    data[commit['key']] = commit['value']
                    continue
            # voterequest
            elif msg_type == 'votereq' and msg['term'] > term:
                leader = 'FFFF'
                if voted == False:
                    vote(source)
            # redirect
            elif msg_type == 'put' or msg_type == 'get':
                out = {"src": my_id, "dst": source, "leader": leader, "type": "redirect", "MID": msg['MID']}
                norm_send(sock, out)
        # heartbeat not heard in a while
        if time.time() - elect_timer > random.randint(650, 900) * 0.001:
            become_cand()
            log('follower becoming candidate')
            continue

    if state == 'candidate':
        if len(msg_raw) > 0:
            msg = json.loads(msg_raw)
            # save the sender's id
            source = msg['src']
            # save the type of message received
            msg_type = msg['type']
            if msg_type == 'append':
                if msg['term'] > term:
                    log('need update')
                    request_update(msg['term'])
                else:
                    append_react(source, msg['data'])
                continue
            # redirect until election ends
            elif msg_type == 'put' or msg_type == 'get':
                out = {"src": my_id, "dst": source, "leader": 'FFFF', "type": "redirect", "MID": msg['MID']}
                norm_send(sock, out)
            # getting votes
            elif msg_type == 'vote':
                votes += 1
                if votes > (len(replica_ids) / 2):
                    become_leader()
                    continue
            # voterequest
            elif msg_type == 'votereq' and msg['term'] > term:
                become_follower('FFFF')
                vote(source)
                continue
        # heartbeat not heard in a while
        if time.time() - elect_timer > random.randint(650, 900) * 0.001:
            become_cand()
            log('why is this happening')
            continue

    if state == 'leader':
        if len(msg_raw) > 0:
            msg = json.loads(msg_raw)
            # save the sender's id
            source = msg['src']
            # save the type of message received
            msg_type = msg['type']
            if msg_type == 'ack':
                if msg['new'] == len(temp_log):
                    acks += 1
                    if acks > (len(replica_ids) / 2):
                        commit_temp()
                        replicas_commit()
            if msg_type == 'append' and msg['term'] > term:
                become_follower(source)
                log('dropped leadership')
                request_update(msg['term'])
                continue
            elif msg_type == 'update':
                send_update(msg['term'], source)
            elif msg_type == 'get':
                flag = False
                super_temp_log.append({'key': msg['key'], 'type': 'get'})
                super_waiting_clients.append((source, msg['MID']))
                log('received GET {} from {}'.format(msg['key'], source))
            elif msg_type == 'put':
                super_keys.append(msg['key'])
                super_temp_log.append({'key': msg['key'], 'value': msg['value'],
                                       'type': 'put', 'id': random.randint(0, 1000000)})
                super_waiting_clients.append((source, msg['MID']))
                log('received PUT {} from {}'.format(msg['key'], source))
            if len(temp_log) == 0:
                # do gets
                while super_temp_log and super_temp_log[0]['type'] == 'get':
                    get = super_temp_log.popleft()
                    who = super_waiting_clients.popleft()
                    who2 = who[1]
                    who = who[0]
                    if get['key'] in data:
                        what = data[get['key']]
                    else:
                        what = ''
                    out = {"src": my_id, "dst": who, "leader": leader,
                     "type": "ok", "MID": who2, "value": what}
                    norm_send(sock, out)

                while super_temp_log and super_temp_log[0]['type'] == 'put':
                    temp_log.append(super_temp_log.popleft())
                    waiting_clients.append(super_waiting_clients.popleft())
                    temp_keys.append(super_keys.popleft())
                if temp_log:
                    send_append()
                    send_time = time.time()

        if time.time() - send_time > 0.5:
            acks = 1
            send_append()
            send_time = time.time()
            log('RESENT')

        if time.time() - heartbeat > 600 * 0.001:
            send_append()
            log('would have heartbeated')






# TODO LIST
# fix leader transfer issues, gate sending abilities to leaders only
# maybe we should send the queue to replicas every second or so?
# during elections, exchange log numbers to ensure the leader is the most current
