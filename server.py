import sys
import random
import concurrent.futures
import threading
import time
import grpc

import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2

#
# constants
#

# [HEARTBEAT_DURATION, ELECTION_DURATION_FROM, ELECTION_DURATION_TO] = [x*10 for x in [50, 150, 300]]
[HEARTBEAT_DURATION, ELECTION_DURATION_FROM, ELECTION_DURATION_TO] = [x for x in [50, 5000, 6000]]

#
# global state
#

is_terminating = False
is_suspended = False
state_lock = threading.Lock()
election_timer_fired = threading.Event()
heartbeat_events = {}
state = {
    'saved_keys': {},
    'logs': [],
    'last_applied': 0,
    'commit_index': 0,
    'next_index': {},
    'match_index': {},
    'election_campaign_timer': None,
    'election_timeout': -1,
    'type': 'follower',
    'nodes': None,
    'term': 0,
    'vote_count': 0,
    'voted_for_id': -1,
    'leader_id': -1,
    'id': -1
}

# for debugging
START_TIME = time.time()


def log_prefix():
    time_since_start = '{:07.3f}'.format(time.time() - START_TIME)
    return f"{state['term']}\t{time_since_start}\t{state['type']}\t[id={state['id']} leader_id={state['leader_id']} vote_count={state['vote_count']} voted_for={state['voted_for_id']}] "


#
# election timer functions
#

def select_election_timeout():
    return random.randrange(ELECTION_DURATION_FROM, ELECTION_DURATION_TO) * 0.001


# def fire_election_timer(id):
#     state['current_timer_id'] = id
#     election_timer_fired.set()

def reset_election_campaign_timer():
    stop_election_campaign_timer()
    state['election_campaign_timer'] = threading.Timer(state['election_timeout'], election_timer_fired.set)
    state['election_campaign_timer'].start()


def select_new_election_timeout_duration():
    state['election_timeout'] = select_election_timeout()


def stop_election_campaign_timer():
    if state['election_campaign_timer']:
        state['election_campaign_timer'].cancel()


#
# elections
#

def start_election():
    with state_lock:
        state['type'] = 'candidate'
        state['leader_id'] = -1
        state['term'] += 1
        # vote for ourselves
        state['vote_count'] = 1
        state['voted_for_id'] = state['id']

    print(f"I am a candidate. Term: {state['term']}")
    for id in state['nodes'].keys():
        if id != state['id']:
            t = threading.Thread(target=request_vote_worker_thread, args=(id,))
            t.start()
    # now RequestVote threads have started,
    # lets set a timer for the end of the election
    reset_election_campaign_timer()


def has_enough_votes():
    required_votes = (len(state['nodes']) // 2) + 1
    return state['vote_count'] >= required_votes


def finalize_election():
    stop_election_campaign_timer()
    with state_lock:
        if state['type'] != 'candidate':
            return

        if has_enough_votes():
            # become a leader
            state['type'] = 'leader'
            state['leader_id'] = state['id']
            state['vote_count'] = 0
            state['voted_for_id'] = -1
            start_heartbeats()
            print("Votes received")
            print(f"I am a leader. Term: {state['term']}")
            return
        # if election was unsuccessful
        # then pick new timeout duration
        become_a_follower()
        select_new_election_timeout_duration()
        reset_election_campaign_timer()


def become_a_follower():
    if state['type'] != 'follower':
        print(f"I am a follower. Term: {state['term']}")
    state['type'] = 'follower'
    state['voted_for_id'] = -1
    state['vote_count'] = 0
    # state['leader_id'] = -1


#
# heartbeats
#

def start_heartbeats():
    for id in heartbeat_events:
        heartbeat_events[id].set()


#
# thread functions
#

def request_vote_worker_thread(id_to_request):
    ensure_connected(id_to_request)
    (_, _, stub) = state['nodes'][id_to_request]
    try:
        index, term = 0, 0
        if len(state['logs']) > 0:
            index = state['logs'][-1]['index']
            term = state['logs'][-1]['term']

        msg = pb2.RequestVoteRequest(term=str(state['term']), candidateId=str(state['id']), lastLogIndex=str(index),
                                     lastLogTerm=str(term))
        resp = stub.RequestVote(msg)

        with state_lock:
            # if requested node replied for too long,
            # and during this time candidate stopped
            # being a candidate, then do nothing
            if state['type'] != 'candidate' or is_suspended:
                return

            if state['term'] < resp.term:
                state['term'] = resp.term
                become_a_follower()
                reset_election_campaign_timer()
            elif resp.result:
                state['vote_count'] += 1

        # got enough votes, no need to wait for the end of the timeout
        if has_enough_votes():
            finalize_election()
    except grpc.RpcError:
        print(f'Did not get massage from {id_to_request}')
        reopen_connection(id_to_request)


def election_timeout_thread():
    while not is_terminating:
        if election_timer_fired.wait(timeout=0.5):
            election_timer_fired.clear()
            if is_suspended:
                continue

            # election timer just fired
            if state['type'] == 'follower':
                # node didn't receive any heartbeats on time
                # that's why it should become a candidate
                print("The leader is dead")
                start_election()
            elif state['type'] == 'candidate':
                # okay, election is over
                # we need to count votes
                finalize_election()
            # if somehow we got here while being a leader,
            # then do nothing


def form_message(id_to_request):
    previous_log_term = 0
    if id_to_request not in state['match_index'].keys():
        state['match_index'][id_to_request] = 0
        state['next_index'][id_to_request] = 1
    elif len(state['logs']) > 0:
        previous_log_term = state['logs'][state['match_index'][id_to_request]]['term']

    logs_to_send = []

    if state['commit_index'] >= state['next_index'][id_to_request]:
        for log in state['logs']:
            if log[0] > state['match_index'][id_to_request]:
                logs_to_send.append(log)

    return previous_log_term, logs_to_send


def max_in_dict(dictionary):
    max = 0
    for key in dictionary.keys():
        if dictionary[key] > max:
            max = dictionary[key]
    return max


def find_N(match_index):
    N = max_in_dict(match_index)
    count = 0
    while N > state['commit_index']:
        for key in match_index.keys():
            if match_index[key] >= N:
                count += 1
        if count > len(state['nodes'].keys()) // 2 and state['logs'][N]['term'] == state['term']:
            state['commit_index'] = N
        else:
            N -= 1


def heartbeat_thread(id_to_request):
    while not is_terminating:
        try:
            if heartbeat_events[id_to_request].wait(timeout=0.5):
                heartbeat_events[id_to_request].clear()

                if (state['type'] != 'leader') or is_suspended:
                    continue

                previous_log_term, logs_to_send = form_message(id_to_request)

                ensure_connected(id_to_request)
                (_, _, stub) = state['nodes'][id_to_request]

                resp = stub.AppendEntries(
                    pb2.AppendEntriesRequest(prevLogIndex=state['match_index'][id_to_request],
                                             prevLogTerm=previous_log_term,
                                             leaderCommit=state['commit_index'], entries=logs_to_send))

                if resp.result:
                    state['match_index'][id_to_request] = state['commit_index']
                    state['next_index'][id_to_request] = state['match_index'][id_to_request] + 1
                else:
                    state['next_index'][id_to_request] -= 1

                find_N(state['match_index'])

                if (state['type'] != 'leader') or is_suspended:
                    continue

                with state_lock:
                    if state['term'] < resp.term:
                        reset_election_campaign_timer()
                        state['term'] = resp.term
                        become_a_follower()
                threading.Timer(HEARTBEAT_DURATION * 0.001, heartbeat_events[id_to_request].set).start()
        except grpc.RpcError:
            reopen_connection(id_to_request)


#
# gRPC server handler
#

# helpers that sets timers running again
# when suspend has ended
def wake_up_after_suspend():
    global is_suspended
    is_suspended = False
    if state['type'] == 'leader':
        start_heartbeats()
    else:
        reset_election_campaign_timer()


class Handler(pb2_grpc.RaftNodeServicer):
    def RequestVote(self, request, context):
        term = int(request.term)
        candidate_id = int(request.candidateId)
        last_log_index = int(request.lastLogIndex)
        last_log_term = int(request.lastLogTerm)

        global is_suspended
        if is_suspended:
            return

        def last_log_term_exists():
            for log in state['logs']:
                if log['index'] == last_log_index:
                    return log['term']
            return -1

        reset_election_campaign_timer()
        with state_lock:
            reply = {'result': False, 'term': state['term']}
            if state['term'] < term:
                state['term'] = term
                become_a_follower()

            if len(state['logs']) > 0:
                if last_log_index < state['logs'][-1]['index']:
                    return pb2.ResultWithTerm(**reply)
                if last_log_term_exists() != last_log_term:
                    return pb2.ResultWithTerm(**reply)

            if state['term'] == term:
                if state['voted_for_id'] == -1:
                    become_a_follower()
                    state['voted_for_id'] = candidate_id
                    reply = {'result': True, 'term': state['term']}
                    print(f"Voted for node {state['voted_for_id']}")

            return pb2.ResultWithTerm(**reply)

    def AppendEntries(self, request, context):
        term = request.term
        leader_id = request.leaderId
        prev_log_index = request.prevLogIndex
        prev_log_term = request.prevLogTerm  # TODO SOMETHING
        entries = request.entries  # entries = [{index, term, command}]
        leader_commit = request.leaderCommit

        global is_suspended
        if is_suspended:
            return

        reset_election_campaign_timer()
        with state_lock:
            reply = {'result': False, 'term': state['term']}

            if prev_log_index is not None:
                for entry in entries:
                    if entry not in state['logs']:
                        for log in state['logs']:
                            if entry['index'] == log['index']:
                                if entry['term'] != log['term'] or entry['command'] != log['command']:
                                    state['logs'].remove(log)
                                break

                        state['logs'].append(entry)

            if leader_commit > state['commit_index']:
                state['commit_index'] = min(leader_commit, entries[-1]['index'])  # TODO i'm not sure about entries

            if state['term'] < term:
                state['term'] = term
                become_a_follower()
            else:
                state['leader_id'] = leader_id
                reply = {'result': True, 'term': state['term']}

            return pb2.ResultWithTerm(**reply)

        # with state_lock:
        #     reply = {'result': False, 'term': state['term']}
        #     if state['term'] < term:
        #         state['term'] = term
        #         become_a_follower()
        #     if state['term'] == term:
        #         state['leader_id'] = leader_id
        #         reply = {'result': True, 'term': state['term']}
        #     return pb2.ResultWithTerm(**reply)

    def GetLeader(self, request, context):
        global is_suspended
        if is_suspended:
            return

        (host, port, _) = state['nodes'][state['leader_id']]
        reply = {'leader_id': state['leader_id'], 'leader_addr': f"{host}:{[port]}"}
        return pb2.LeaderResp(**reply)

    def Suspend(self, request, context):
        global is_suspended
        if is_suspended:
            return

        is_suspended = True
        print(f'Suspended for {request.duration} seconds')
        threading.Timer(request.duration, wake_up_after_suspend).start()
        return pb2.NoArgs()

    def SetValue(self, request, context):
        key = request.key
        value = request.value
        if state['type'] == 'leader':
            try:
                state['logs'].append({'index': state['last_applied'] + 1, 'term': state['term'],
                                      'command': ('set', key, value)})
                state['last_applied'] += 1

                # TODO replicate log
                reply = {'result': False}
                answers = []
                for id in state['nodes'].keys():
                    ensure_connected(id)
                    (host, port, stub) = state['nodes'][id]
                    msg = pb2.Entry(index=state['last_applied'] + 1, term=state['term'], command=('set', key, value))
                    replicate = stub.Replicate(msg)
                    answers.append(replicate.response)

                if sum(answers) > len(state['nodes'].keys()) // 2:  # TODO does it really need the votes
                    state['saved_keys'][key] = value
                    state['last_applied'] += 1
                    reply = {'result': True}
                    for id in state['nodes'].keys():
                        ensure_connected(id)
                        (host, port, stub) = state['nodes'][id]
                        msg = pb2.KeyValue(key=key, value=value)
                        stub.ConfirmReplication(msg)

                return pb2.BoolResult(**reply)
            except:
                reply = {'result': False}
                return pb2.BoolResult(**reply)
        elif state['type'] == 'follower':
            reopen_connection(state['leader_id'])
            state['nodes'][state['leader_id']][-1].SetValue(request)
        else:
            reply = {'result': False}
            return pb2.BoolResult(**reply)

    def Replicate(self, request, context):
        index = request.index
        term = request.term
        command = request.command

        state['logs'].append({'index': index, 'term': term, 'command': command})
        reply = {'result': True}
        return pb2.BoolResult(**reply)

    def ConfirmReplication(self, request, context):
        key = request.key
        value = request.value
        state['saved_keys'][key] = value
        return pb2.NoArgs()

    def GetValue(self, request, context):
        key = request.key
        if key in state['saved_keys'].keys():
            reply = {'value': state['saved_keys'][key], 'result': True}
            return pb2.Value(**reply)
        reply = {'result': False}
        return pb2.Value(**reply)


def ensure_connected(id):
    if id == state['id']:
        raise "Shouldn't try to connect to itself"
    (host, port, stub) = state['nodes'][id]
    if not stub:
        channel = grpc.insecure_channel(f"{host}:{port}")
        stub = pb2_grpc.RaftNodeStub(channel)
        state['nodes'][id] = (host, port, stub)


def reopen_connection(id):
    if id == state['id']:
        raise "Shouldn't try to connect to itself"
    (host, port, stub) = state['nodes'][id]
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = pb2_grpc.RaftNodeStub(channel)
    state['nodes'][id] = (host, port, stub)


def start_server(state):
    (ip, port, _stub) = state['nodes'][state['id']]
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftNodeServicer_to_server(Handler(), server)
    server.add_insecure_port(f"{ip}:{port}")
    server.start()
    return server


def main(id, nodes):
    election_th = threading.Thread(target=election_timeout_thread)
    election_th.start()

    heartbeat_threads = []
    for node_id in nodes:
        if id != node_id:
            heartbeat_events[node_id] = threading.Event()
            t = threading.Thread(target=heartbeat_thread, args=(node_id,))
            t.start()
            heartbeat_threads.append(t)

    state['id'] = id
    state['nodes'] = nodes
    state['type'] = 'follower'
    state['term'] = 0

    server = start_server(state)
    (host, port, _) = nodes[id]
    print(f"The server starts at {host}:{port}")
    print(f"I am a follower. Term: 0")
    select_new_election_timeout_duration()
    reset_election_campaign_timer()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        global is_terminating
        is_terminating = True
        server.stop(0)
        print("Shutting down")

        election_th.join()
        [t.join() for t in heartbeat_threads]


if __name__ == '__main__':
    [id] = sys.argv[1:]
    nodes = None
    with open("config.conf", 'r') as f:
        line_parts = map(lambda line: line.split(), f.read().strip().split("\n"))
        nodes = dict([(int(p[0]), (p[1], int(p[2]), None)) for p in line_parts])
        print(list(nodes))
    main(int(id), nodes)
