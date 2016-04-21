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
log_num = 0
logging = collections.deque()
# the keys and values stored on the replica: <dictionary>
data = {}
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
preparing = False
# number of ready to commit replicas
readyReps = 0
# time until send next heartbeat
heartbeat = 0
# time since we've sent commit data to replicas
send_time = time.time()
# ready to commit on go
commit = -1

committing_clients = []
put_id = -1

quorum = (len(replica_ids) / 2)

msg_queue = collections.deque()


class Put():
    def __init__(self, key, val, source, mid, type, put_id):
        self.key = key
        self.source = source
        self.mid = mid
        self.type = type
        self.val = val
        self.put_id = put_id


def log(msg):
    if prints == True:
        print '{}: {}'.format(datetime.datetime.now(), msg)


def restore():
    msg = {'src': my_id, 'dst': leader, 'leader': leader, 'type': 'restore', 'log': log_num, 'term': term}
    sock.send(json.dumps(msg))
    log('{} sending a data request to {}'.format(msg['src'], msg['dst']))


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

def begin_election():
    global term, lastrec, leader, votes, promises, log_num, heartbeat
    log('{} starting new election'.format(my_id))
    leader = 'FFFF'
    # increase the term number
    term += 1
    # reset any past votes
    votes = 0
    promises = 0
    heartbeat = 0
    # send a broadcast to all replicas to make me the leader
    s_msg = {'src': my_id, 'dst': 'FFFF', 'leader': 'FFFF', 'type': 'votereq', 'log': log_num, 'term': term}
    norm_send(sock, s_msg)
    log('{} sending a vote request to FFFF'.format(my_id))
    lastrec = time.time()

log(('Replica {} starting up').format(my_id))

while True:

    msg_raw = check_socket(sock)

    if len(msg_raw) > 0:
        msg = json.loads(msg_raw)
        # save the sender's id
        source = msg['src']
        # save the type of message received
        msg_type = msg['type']
        # handle put messages, send back redirect or add to to-do list

        if leader != my_id and (msg_type == 'put' or msg_type == 'get'):
            log('{} redirecting user {} to leader {} {}'.format(my_id, source, leader, msg_type.upper()))
            s_msg = {'src': my_id, 'dst': source, 'leader': leader, 'type': 'redirect', 'MID': msg['MID']}
            norm_send(sock, s_msg)

        # place PUT requests in queue
        elif msg_type == 'put':
            key = msg['key']
            val = msg['value']
            mid = msg['MID']
            msg_info = (key, val, source, mid, msg_type)
            msg_queue.append(msg_info)
            log(('leader {} received a PUT request from user {}').format(my_id, source))


        # place GET request in queue
        elif msg_type == 'get':
            key = msg['key']
            mid = msg['MID']
            msg_info = (key, -1, source, mid, msg_type)
            msg_queue.append(msg_info)
            log('{} received a GET request from user {}'.format(my_id, source))

        # handle ready messages, when enough are acquired, commit the change
        elif msg_type == 'ready' and msg['log'] <= log_num and msg['term'] <= term and leader == my_id:
            readyReps += 1
            send_time = time.time()
            log('{} recieved ready from replica {}'.format(my_id, source))
            # if quorum has been established
            if readyReps > quorum:
                readyReps = 0
                # increase the number of committed messages
                log_num += len(val_list)
                i = 0
                for val in val_list:
                    logging.append((key_list[i], val_list[i]))
                    i += 1
                logging.append((key_list))
                log('{} is committing a change'.format(my_id))
                # commit the change
                for num in xrange(len(key_list)):
                    data[key_list[num]] = val_list[num]
                # send the commit alert to all replicas
                s_msg = {'src': my_id, 'dst': 'FFFF', 'leader': leader, 'type': 'commit', 'log': log_num,
                         'keys': key_list, 'values': val_list, 'term': term, 'put_id': put_id}
                send_as_leader(sock, s_msg)
                # indicate that we are ready to commit more messages
                preparing = False
                '''
                for client in committing_clients:
                    ns_msg = {'src': my_id, 'dst': client[0], 'leader': leader, 'type': 'ok', 'MID': client[1]}
                    norm_send(sock, ns_msg)
                log('{} sending a PUT confirmation to {} users'.format(my_id, len(committing_clients)))
                committing_clients = []
                '''

        # handle vote messages when attempting to become the leader
        elif msg_type == 'vote' and msg['term'] == term:
            lastrec = time.time()
            votes += 1
            # TODO maybe just have the voter hold a new election when it realizes
            '''if log_num < msg['log']:
                s_msg = {'src': my_id, 'dst': source, 'leader': 'FFFF', 'type': 'elect', 'id': source}
                norm_send(sock, s_msg)
                log('{} sending an elect request to {}'.format(my_id, source))'''
            if votes > quorum:
                votes = 0
                # decree my new reign
                log('I, replica # {}, am the leader!'.format(my_id))
                leader = my_id
                # alert the peasants of their new king
                s_msg = {'src': my_id, 'dst': 'FFFF', 'leader': leader, 'type': 'promreq', 'log': log_num, 'term': term}
                send_as_leader(sock, s_msg)
                log('{} sending a promise request to {}'.format(my_id, 'FFFF'))

        # handle promise messages when establishing a quorum
        elif msg_type == 'prom' and msg['term'] >= term:
            lastrec = time.time()
            promises += 1
            if promises > (len(replica_ids) / 2) + 1:
                log('quorum has been established, commencing request execution')

        #elif msg_type == 'restore':
        #    s_msg = {'src': my_id, 'dst': source, 'leader': my_id, 'type': 'restoration', 'log': log_num, 'term': term,
        #           'data': data}
        #    norm_send(sock, s_msg)


        # handle info messages, send back 'ready to send'
        elif msg_type == 'prepare' and msg['log'] >= log_num and msg['term'] >= term:
            lastrec = time.time()

            # get ready for commit
            #commit_pair = {'key': msg['key'], 'value': msg['value'], 'id': msg['put_id']}
            commit = msg['put_id']
            # send confirmation to the leader that I am ready to commit
            s_msg = {'src': my_id, 'dst': leader, 'leader': leader, 'type': 'ready', 'log': log_num,
                     'term': term, 'id': commit}
            norm_send(sock, s_msg)
            log('{} sending ready to commit my new commit id is {}'.format(my_id, commit))



        elif msg_type == 'commit' and msg['log'] >= log_num and msg['term'] >= term:
            lastrec = time.time()
            # if the replica has the value in temp storage
            '''if msg['put_id'] == commit['id']:
                log_num += 1
                log('{} is committing a change'.format(my_id))
                data[commit_pair['key']] = commit_pair['value']
            # if the commit does not match the ready id
            else:
                log('{} cannot commit the change!'.format(my_id))'''
            keys = msg['keys']
            vals = msg['values']
            for num in xrange(len(keys)):
                log_num += 1
                logging.append((keys[num], vals[num]))
                data[keys[num]] = vals[num]
            log('{} is committing a change for commit id {}'.format(my_id, commit))


        # handle vote request messages, send back a vote
        elif msg_type == 'votereq' and msg['term'] > term:
            leader = 'FFFF'
            term += 1
            lastrec = time.time()
            log('{} received a vote request from {}'.format(my_id, source))
            # send the vote to the candidate
            s_msg = {'src': my_id, 'dst': source, 'leader': 'FFFF', 'type': 'vote', 'log': log_num, 'term': term}
            norm_send(sock, s_msg)
            log('{} sending my vote to {}'.format(my_id, source))



        elif msg_type == 'elect':
            lastrec = time.time()
            # this allows the most current replica to start the election
            if msg['id'] == my_id:
                lastrec = 0

        # handle promise request messages when building a quorum
        elif msg_type == 'promreq':
            votes = 0
            term = msg['term']
            #if log_num < msg['log']:
            #    restore()
            lastrec = time.time()
            # acknowledge the new leader as such
            leader = source
            log('{} received a promise request from {}'.format(my_id, source))
            # send a pledge to the leader
            s_msg = {'src': my_id, 'dst': source, 'leader': leader, 'type': 'prom', 'log': log_num, 'term': term}
            norm_send(sock, s_msg)
            log('{} sending my promise to {}'.format(my_id, source))


        elif msg_type == 'heartbeat':
            # catch up an out dated replica
            #if log_num < msg['log']:
            #    restore()
            lastrec = time.time()
            leader = source


        elif msg_type == 'restoration':
            data = msg['data']
            term = msg['term']
            log_num = msg['log']
            log('{} restored to current data from leader {}'.format(my_id, source))


    if leader == my_id:

        # keep followers aware of the leaders existence
        if (time.time() - heartbeat) > 0.5:
            s_msg = {'src': my_id, 'dst': 'FFFF', 'leader': my_id, 'type': 'heartbeat', 'log': log_num, 'term': term}
            send_as_leader(sock, s_msg)
            log('{} sending a heartbeat to FFFF'.format(my_id))

        # if we have put requests in our to-do list
        if not preparing:
            if msg_queue:
                cur = 'put'
                send_queue = collections.deque()

                key_list = []
                val_list = []

                flux_keys = []
                temp_queue = collections.deque()
                for mess in msg_queue:
                    m_type = mess[4]
                    m_key = mess[0]
                    if m_type == 'put':
                        flux_keys.append(m_key)
                    if m_type == 'get' and (m_key not in flux_keys):
                        if m_key not in flux_keys:
                            g_key = m_key
                            g_client = mess[2]
                            g_msg_id = mess[3]
                            if g_key in data:
                                send_value = data[g_key]
                            else:
                                send_value = ''
                            s_msg = {'src': my_id, 'dst': g_client, 'leader': leader, 'type': 'ok', 'MID': g_msg_id, 'value': send_value}
                            norm_send(sock, s_msg)
                            log('{} sending a GET confirmation to user {}'.format(my_id, g_client))
                    else:
                        temp_queue.append(mess)

                msg_queue = temp_queue

                counter = 5
                while len(msg_queue) > 0 and msg_queue[0][4] == 'put' and counter > 0:
                    send_queue.append(msg_queue.popleft())
                    counter -= 1

                if len(send_queue) > 0:
                    for put in send_queue:
                        key_list.append(put[0])
                        val_list.append(put[1])
                        committing_clients.append((put[2], put[3]))
                        ns_msg =  {'src': my_id, 'dst': put[2], 'leader': leader, 'type': 'ok', 'MID': put[3]}
                        norm_send(sock, ns_msg)
                    log('{} sending a PUT confirmation to {} users'.format(my_id, len(committing_clients)))
                    committing_clients = []
                    preparing = True
                    readyReps = 0
                    put_id = random.randint(1, 10000000000)
                    s_msg = {'src': my_id, 'dst': 'FFFF', 'leader': my_id, 'type': 'prepare', 'log': log_num, 'term': term, 'put_id': put_id}
                    send_as_leader(sock, s_msg)
                    send_time = time.time()
                    log('{} sending data to all replicas'.format(my_id))




        # if we haven't committed something, must resend the info
        if preparing and (time.time() - send_time) > .3:
            readyReps = 0
            s_msg = {'src': my_id, 'dst': 'FFFF', 'leader': my_id, 'type': 'prepare',
                   'log': log_num, 'term': term, 'put_id': put_id}
            send_as_leader(sock, s_msg)
            send_time = time.time()
            log('{} RE-sending data to all replicas'.format(my_id))


    else:
        # if the time since the last message is between 550 - 800 milliseconds we must check on the leader, then start elections
        if time.time() - lastrec > (random.randint(550, 800) * .001) and not leader == my_id:
            begin_election()




# TODO LIST
# fix leader transfer issues, gate sending abilities to leaders only
# maybe we should send the queue to replicas every second or so?
# during elections, exchange log numbers to ensure the leader is the most current
