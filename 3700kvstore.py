#!/usr/bin/env python

import sys, socket, select, time, json, random, datetime

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
# the log of changes committed by replicas
log = []
# the keys and values stored on the replica
data = []
# a list of requests from users, used by the leader
requests = []
# the time of the most recently received message
lastrec = time.time()
# the number of votes received by this replica
votes = 0
# the number of promises received by this replica
promises = 0
# the id of current leader
leader = -1


def log(msg):
        print '{}: {}'.format(datetime.datetime.now(), msg)

log(('Replica {} starting up').format(my_id))

while True:
        ready = select.select([sock], [], [], 0.1)[0]

        if sock in ready:
                msg_raw = sock.recv(32768)

                if len(msg_raw) == 0: continue
                # tracks the time of the last recieved message
                lastrec = time.time()
                msg = json.loads(msg_raw)
                # save the sender's id
                source = msg['src']
                type = msg['type']

                # For now, ignore get() and put() from clients
                if type in ['get', 'put']:
                        pass

                # handle votereq messages, send back a vote
                elif type == 'votereq':
                        log('%s received a vote request from %s' % (my_id, msg['src']))
                        # send the vote to the candidate
                        msg = {'src': my_id, 'dst': source, 'leader': 'FFFF', 'type': 'vote'}
                        sock.send(json.dumps(msg))
                        log('%s sending my vote to %s' % (msg['src'], msg['dst']))

                # handle vote messages when attempting to become the leader
                elif type == 'vote':
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
                elif type == 'promreq':
                        # acknowledge the new leader as such
                        leader = source
                        log('%s received a promise request from %s' % (my_id, msg['src']))
                        # send a pledge to the leader
                        msg = {'src': my_id, 'dst': source, 'leader': source, 'type': 'prom'}
                        sock.send(json.dumps(msg))
                        log('%s sending my promise to %s' % (msg['src'], msg['dst']))

                elif type == 'prom':
                        promises += 1
                        if promises > (len(replica_ids) / 2) + 1:
                                log('quorum has been established, commencing request execution')

        # if the time since the last message is between 150 - 300 milliseconds we must start elections
        if time.time() - lastrec > (random.randint(150, 300) * .001):
                # increase the term number
                term += 1
                # reset any past votes
                votes = 0
                # send a broadcast to all replicas to make me the leader
                msg = {'src': my_id, 'dst': 'FFFF', 'leader': 'FFFF', 'type': 'votereq'}
                sock.send(json.dumps(msg))
                log('%s sending a vote request to %s' % (msg['src'], msg['dst']))

        # clock = time.time()
        # if clock-last > 2:
        #         # Send a no-op message to a random peer every two seconds, just for fun
        #         # You definitely want to remove this from your implementation
        #         msg = {'src': my_id, 'dst': random.choice(replica_ids), 'leader': 'FFFF', 'type': 'noop'}
        #         sock.send(json.dumps(msg))
        #         print '%s sending a NOOP to %s' % (msg['src'], msg['dst'])
        #         last = clock
