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
# the current term of leadership
term = 0
# the log # of changes committed by replicas
logNum = 0
# the keys and values stored on the replica: <dictionary>
data = {}
tempKey = -1
tempValue = -1
# a deque of clients to send feedback to
client = -1
msg_id = -1
# the time of the most recently received message
lastrec = time.time()
# the number of votes received by this replica
votes = 0
# the number of promises received by this replica
promises = 0
# the id of current leader
leader = 'FFFF'
# print status
prints = True
# if we are in the process of a commit or not
sending = False
# number of ready to commit replicas
readyReps = 0
# if we haven't heard from the leader, singals start of an election
panic = False
# time since we've sent commit data to replicas
send_time = time.time()

msg_queue = collections.deque()


def log(msg):
        if prints == True:
                print '{}: {}'.format(datetime.datetime.now(), msg)

log(('Replica {} starting up').format(my_id))

while True:
        ready = select.select([sock], [], [], 0.1)[0]

        if sock in ready:
                msg_raw = sock.recv(32768)
                if len(msg_raw) == 0: continue
                msg = json.loads(msg_raw)
                # save the sender's id
                source = msg['src']
                # save the type of message received
                msgtype = msg['type']
                # handle put messages, send back redirect or add to to-do list
                if msgtype == 'put':
                        key = msg['key']
                        val = msg['value']
                        id = msg['MID']
                        if leader == my_id:
                                msg_info = (key, val, source, id, msgtype)
                                msg_queue.append(msg_info)
                                log(('leader {} received a PUT request from user {}').format(my_id, source))
                        # if the request goes to another replica, alert the user of the leaders location for redirect
                        else:
                                log(('{} received a PUT request from user {}').format(my_id, source))
                                msg = {'src': my_id, 'dst':  source, 'leader': leader, 'type': 'redirect', 'MID': id}
                                sock.send(json.dumps(msg))
                                log('%s sending a redirect request to user %s' % (msg['src'], msg['dst']))


                # handle get messages, send back response
                if msgtype == 'get':
                        log(('{} received a GET request from user {}').format(my_id, source))
                        id = msg['MID']
                        if leader == my_id:
                                key = msg['key']
                                id = msg['MID']
                                msg_info = (key, -1, source, id, msgtype)
                                msg_queue.append(msg_info)
                        else:
                                log(('{} received a GET request from user {}').format(my_id, source))
                                msg = {'src': my_id, 'dst':  source, 'leader': leader, 'type': 'redirect', 'MID': id}
                                sock.send(json.dumps(msg))
                                log('%s sending a redirect request to user %s' % (my_id, source))

                # handle info messages, send back 'ready to send'
                if msgtype == 'info' and msg['log'] >= logNum and msg['term'] >= term:
                        lastrec = time.time()
                        # store the values in the temp dictionary
                        tempValue = msg['value']
                        # append the key to the deque so its in the to-do list
                        tempKey = msg['key']
                        # send confirmation to the leader that I am ready to commit
                        msg = {'src': my_id, 'dst':  leader, 'leader': leader, 'type': 'ready', 'log': logNum, 'term': term}
                        sock.send(json.dumps(msg))
                        log(('{} sending ready to commit').format(my_id))

                # handle ready messages, when enough are acquired, commit the change
                if msgtype == 'ready' and msg['log'] >= logNum and msg['term'] >= term:
                        readyReps += 1
                        send_time = time.time()
                        # if quorum has been established
                        if readyReps > (len(replica_ids) / 2) + 1:
                                # TODO figure this stuff out so future readies dont reactivate this before
                                # a new PUT
                                readyReps = 0
                                # increase the number of committed messages
                                logNum += 1
                                log(('{} is committing a change').format(my_id))
                                # commit the change
                                data[tempKey] = tempValue
                                # send the commit alert to all replicas
                                msg = {'src': my_id, 'dst': 'FFFF', 'leader': leader, 'type': 'commit', 'key': tempKey, 'log': logNum, 'term': term}
                                sock.send(json.dumps(msg))
                                hearbeat = time.time()
                                # indicate that we are ready to commit more messages
                                sending = False
                                msg = {'src': my_id, 'dst':  client, 'leader': leader, 'type': 'ok', 'MID': msg_id}
                                sock.send(json.dumps(msg))
                                log('%s sending a PUT confirmation to user %s' % (msg['src'], msg['dst']))


                if msgtype == 'commit' and msg['log'] >= logNum and msg['term'] >= term:
                        lastrec = time.time()
                        # if the replica has the value in temp storage
                        if msg['key'] == tempKey:
                                logNum += 1
                                log(('{} is committing a change').format(my_id))
                                data[tempKey] = tempValue
                        # if the replica does not have the value
                        else:
                                log(('{} cannot commit the change!').format(my_id))


                # handle vote request messages, send back a vote
                elif msgtype == 'votereq' and msg['term'] > term:
                        # TODO term stuff
                        term += 1
                        lastrec = time.time()
                        log('%s received a vote request from %s' % (my_id, msg['src']))
                        # send the vote to the candidate
                        msg = {'src': my_id, 'dst': source, 'leader': 'FFFF', 'type': 'vote', 'log': logNum, 'term': term}
                        sock.send(json.dumps(msg))
                        log('%s sending my vote to %s' % (msg['src'], msg['dst']))

                # handle vote messages when attempting to become the leader
                elif msgtype == 'vote' and msg['term'] == term:
                        leader = 'FFFF'
                        lastrec = time.time()
                        votes += 1
                        if votes > (len(replica_ids) / 2) + 1:
                                # decree my new reign
                                log(('I, replica # {}, am the leader!').format(my_id))
                                leader = my_id
                                # alert the peasants of their new king
                                msg = {'src': my_id, 'dst': 'FFFF', 'leader': my_id, 'type': 'promreq', 'log': logNum, 'term': term}
                                sock.send(json.dumps(msg))
                                log('%s sending a promise request to %s' % (msg['src'], msg['dst']))

                # handle promise request messages when building a quorum
                elif msgtype == 'promreq':
                        term = msg['term']
                        #TODO get stuff if your log number is behind
                        lastrec = time.time()
                        # acknowledge the new leader as such
                        leader = source
                        log('%s received a promise request from %s' % (my_id, msg['src']))
                        # send a pledge to the leader
                        msg = {'src': my_id, 'dst': source, 'leader': leader, 'type': 'prom', 'log': logNum, 'term': term}
                        sock.send(json.dumps(msg))
                        log('%s sending my promise to %s' % (msg['src'], msg['dst']))

                # handle promise messages when establishing a quorum
                elif msgtype == 'prom' and msg['term'] >= term:
                        lastrec = time.time()
                        promises += 1
                        if promises > (len(replica_ids) / 2) + 1:
                                log('quorum has been established, commencing request execution')

                elif msgtype == 'heartbeat':
                        #TODO update old replicas
                        lastrec = time.time()
                        panic = False
                        leader = msg['src']

                elif msgtype == 'check':
                        msg = {'src': my_id, 'dst': msg['src'], 'leader': my_id, 'type': 'heartbeat', 'log': logNum, 'term': term}
                        sock.send(json.dumps(msg))
                        log('%s sending a heartbeat to %s' % (msg['src'], msg['dst']))


        # if the time since the last message is between 150 - 300 milliseconds we must check on the leader, then start elections
        if time.time() - lastrec > (random.randint(150, 300) * .001) and not leader == my_id:
                if panic == True or leader == 'FFFF':
                        log(('{} starting new election').format(my_id))
                        leader = 'FFFF'
                        # increase the term number
                        term += 1
                        # reset any past votes
                        votes = 0
                        promises = 0
                        # send a broadcast to all replicas to make me the leader
                        msg = {'src': my_id, 'dst': 'FFFF', 'leader': 'FFFF', 'type': 'votereq', 'log': logNum, 'term': term}
                        sock.send(json.dumps(msg))
                        log('%s sending a vote request to %s' % (msg['src'], msg['dst']))
                else:
                        panic = True
                        msg = {'src': my_id, 'dst': leader, 'leader': leader, 'type': 'check', 'log': logNum, 'term': term}
                        sock.send(json.dumps(msg))
                        log('%s sending a check to %s' % (msg['src'], msg['dst']))
                lastrec = time.time()

        # if we have put requests in our to-do list
        if not sending and msg_queue and leader == my_id:
                readyReps = 0
                req = msg_queue.popleft()
                tempKey = req[0]
                tempValue = req[1]
                client = req[2]
                msg_id = req[3]
                if req[4] == 'get':
                        if tempKey in data:
                                msg = {'src': my_id, 'dst':  client, 'leader': leader, 'type': 'ok', 'MID': msg_id,
                                       'value': data[tempKey]}
                                sock.send(json.dumps(msg))
                                log('%s sending a GET confirmation to user %s' % (my_id, source))
                        # TODO what if the key doesnt exist
                elif req[4] == 'put':
                        sending = True
                        msg = {'src': my_id, 'dst':  'FFFF', 'leader': my_id, 'type': 'info', 'key': tempKey, 'value': tempValue, 'log': logNum, 'term': term}
                        sock.send(json.dumps(msg))
                        send_time = time.time()
                        log('%s sending data to all replicas' % (my_id))

        # if we haven't committed something, must resend the info
        if sending and (time.time() - send_time) > .150:
                        msg = {'src': my_id, 'dst':  'FFFF', 'leader': my_id, 'type': 'info', 'key': tempKey, 'value': tempValue, 'log': logNum, 'term': term}
                        sock.send(json.dumps(msg))
                        log('%s RE-sending data to all replicas' % (my_id))

# TODO LIST
# fix leader transfer issues, gate sending abilities to leaders only
# if a replica is behind, give the leaders log to it
# during elections, exchange log numbers to ensure the leader is the most current


