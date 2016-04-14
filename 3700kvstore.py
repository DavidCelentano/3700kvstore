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
log = 0
# the keys and values stored on the replica: <dictionary>
data = {}
# a list of requests from users, used by the leader
requests = {}
# a deque of requests to be executed
todo = collections.deque()
# a deque of clients to send feedback to
clients = collections.deque()
# the time of the most recently received message
lastrec = time.time()
# the number of votes received by this replica
votes = 0
# the number of promises received by this replica
promises = 0
# the id of current leader
leader = 'FFFF'
# timer to send hearbeats to followers
heartbeat = 0
# print status
prints = False

def log(msg):
        if prints == True:
                print '{}: {}'.format(datetime.datetime.now(), msg)

log(('Replica {} starting up').format(my_id))

while True:
        ready = select.select([sock], [], [], 0.1)[0]

        if sock in ready:
                msg_raw = sock.recv(32768)

                if len(msg_raw) == 0: continue
                # tracks the time of the last received message

                msg = json.loads(msg_raw)
                # save the sender's id
                source = msg['src']
                # save the type of message received
                msgtype = msg['type']

                # handle put messages, send back redirect or add to to-do list
                if msgtype == 'put':
                        # keep track of the message id for redirection
                        msgid = msg['MID']
                        msgkey = msg['key']
                        msgvalue = msg['value']
                        # if the request has reached the leader, add the pair to the requests dictionary,
                        # add the reference id to the to-do list and add the client so we can respond later
                        if leader == my_id:
                                log(('leader {} received a PUT request from user {}').format(my_id, source))
                                # save the data to the requests dictionary
                                requests[msg['key']] = msg['value']
                                # save the key for reference in the to-do list
                                todo.append(msg['key'])
                                # keeps track of the source and msgid for confirmation
                                clients.append((source, msgid))
                                # remove this later TODO --------------- Need to send these changes to all replicas
                                # data[msg['key']] = msg['value']
                                # msg = {'src': my_id, 'dst':  source, 'leader': leader, 'type': 'ok', 'MID': msgid}
                                # sock.send(json.dumps(msg))
                                # log('%s sending a put confirmation to user %s' % (msg['src'], msg['dst']))
                                # TODO this a filthy hack, remove it later, just using this for the milestone and testing
                                msg = {'src': my_id, 'dst':  'FFFF', 'leader': leader, 'type': 'info', 'key': msgkey, 'value': msgvalue}
                                sock.send(json.dumps(msg))
                                log('%s sending data to all replicas' % (msg['src']))

                        # if the request goes to another replica, alert the user of the leaders location for redirect
                        else:
                                log(('{} received a PUT request from user {}').format(my_id, source))
                                msg = {'src': my_id, 'dst':  source, 'leader': leader, 'type': 'redirect', 'MID': msgid}
                                sock.send(json.dumps(msg))
                                log('%s sending a redirect request to user %s' % (msg['src'], msg['dst']))

                # TODO this a filthy hack, remove it later, just using this for the milestone and testing
                if msgtype == 'info':
                        # store the values in the temp dictionary
                        request[msg['key']] = msg['value']
                        # append the key to the deque so its in the to-do list
                        todo.append(msg['key'])
                        # send confirmation to the leader that I am ready to commit
                        msg = {'src': my_id, 'dst':  leader, 'leader': leader, 'type': 'ready', 'key': msgkey, 'value': msgvalue}
                        sock.send(json.dumps(msg))
                        log('%s sending data to all replicas' % (msg['src']))


                # handle get messages, send back response
                if msgtype == 'get':
                        # keep track of the message id for redirection
                        msgid = msg['MID']
                        # keep track of the message key for lookup
                        msgkey = msg['key']
                        log(('{} received a GET request from user {}').format(my_id, source))
                        # if the key exists, return the value
                        if msgkey in data and leader == my_id:
                                msg = {'src': my_id, 'dst':  source, 'leader': leader, 'type': 'ok', 'MID': msgid,
                                       'value': data[msgkey]}
                                sock.send(json.dumps(msg))
                                log('%s sending a get confirmation to user %s' % (msg['src'], msg['dst']))
                        elif msgkey in data:
                                log(('{} received a GET request from user {}').format(my_id, source))
                                msg = {'src': my_id, 'dst':  source, 'leader': leader, 'type': 'redirect', 'MID': msgid}
                                sock.send(json.dumps(msg))
                                log('%s sending a redirect request to user %s' % (msg['src'], msg['dst']))
                        else:
                                msg = {'src': my_id, 'dst':  source, 'leader': leader, 'type': 'fail', 'MID': msgid}
                                sock.send(json.dumps(msg))
                                log('%s sending a get failure to user %s' % (msg['src'], msg['dst']))
                                #print 'Current Data: {}'.format(data)
                                #print 'Requested data: {}'.format(msgkey)

                # handle vote request messages, send back a vote
                elif msgtype == 'votereq':
                        # TODO term stuff
                        # term += 1
                        lastrec = time.time()
                        log('%s received a vote request from %s' % (my_id, msg['src']))
                        # send the vote to the candidate
                        msg = {'src': my_id, 'dst': source, 'leader': 'FFFF', 'type': 'vote'}
                        sock.send(json.dumps(msg))
                        log('%s sending my vote to %s' % (msg['src'], msg['dst']))

                # handle vote messages when attempting to become the leader
                elif msgtype == 'vote':
                        lastrec = time.time()
                        votes += 1
                        if votes > (len(replica_ids) / 2) + 1:
                                # decree my new reign
                                log(('I, replica # {}, am the leader!').format(my_id))
                                leader = my_id
                                # alert the peasants of their new king
                                msg = {'src': my_id, 'dst': 'FFFF', 'leader': my_id, 'type': 'promreq'}
                                sock.send(json.dumps(msg))
                                log('%s sending a promise request to %s' % (msg['src'], msg['dst']))

                # handle promise request messages when building a quorum
                elif msgtype == 'promreq':
                        lastrec = time.time()
                        # acknowledge the new leader as such
                        leader = source
                        log('%s received a promise request from %s' % (my_id, msg['src']))
                        # send a pledge to the leader
                        msg = {'src': my_id, 'dst': source, 'leader': source, 'type': 'prom'}
                        sock.send(json.dumps(msg))
                        log('%s sending my promise to %s' % (msg['src'], msg['dst']))

                # handle promise messages when establishing a quorum
                elif msgtype == 'prom':
                        lastrec = time.time()
                        promises += 1
                        if promises > (len(replica_ids) / 2) + 1:
                                log('quorum has been established, commencing request execution')

                elif msgtype == 'heartbeat':
                        lastrec = time.time()

        # send a hearbeat to keep replicas updated
        if leader == my_id and (time.time() - heartbeat) > .1:
                hearbeat = time.time()
                msg = {'src': my_id, 'dst': 'FFFF', 'leader': my_id, 'type': 'heartbeat'}
                sock.send(json.dumps(msg))
                log('%s sending a heartbeat to %s' % (msg['src'], msg['dst']))

        # if the time since the last message is between 150 - 300 milliseconds we must start elections
        if time.time() - lastrec > (random.randint(150, 300) * .001) and not leader == my_id:
                # increase the term number
                term += 1
                # reset any past votes
                votes = 0
                promises = 0
                # send a broadcast to all replicas to make me the leader
                msg = {'src': my_id, 'dst': 'FFFF', 'leader': 'FFFF', 'type': 'votereq'}
                sock.send(json.dumps(msg))
                log('%s sending a vote request to %s' % (msg['src'], msg['dst']))

        # if we have put requests in our to-do list
        #if len(todo) > 0:



# when receiving instructions, always check the term number
# if a replica is behind, give the leaders log to it
# during elections, exchange log numbers to ensure the leader is the most current
# should exchange log and term numbers in all communications, besides those to clients
#

