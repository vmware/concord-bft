# Concord
#
# Copyright (c) 2019 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

import time
from enum import Enum
from util import skvbc as kvbc, eliot_logging as log
from util import bft
import trio
import random
from functools import wraps
from util.skvbc_exceptions import(
    ConflictingBlockWriteError,
    StaleReadError,
    NoConflictError,
    InvalidReadError,
    PhantomBlockError,
    ReadTimeoutError
)

MAX_LOOKBACK=10


def verify_linearizability(pre_exec_enabled=False, no_conflicts=False, block_Accumulation=False):
    """
    Creates a tracker and provide it to the decorated method.
    In the end of the method it checks the linearizability of the resulting history.
    """
    def decorator(async_fn):
        @wraps(async_fn)
        async def wrapper(*args, **kwargs):
            if 'disable_linearizability_checks' in kwargs:
                kwargs.pop('disable_linearizability_checks')
                log.log_message(message_type=f'Disabling linearizability is deprecated')
                
            bft_network = kwargs['bft_network']
            skvbc = kvbc.SimpleKVBCProtocol(bft_network)
            init_state = skvbc.initial_state()
            tracker = SkvbcTracker(init_state, skvbc, bft_network, pre_exec_enabled, no_conflicts, block_Accumulation)
            skvbc.tracker = tracker
            skvbc.pre_exec_all = pre_exec_enabled
            await async_fn(*args, **kwargs, tracker=tracker)
            await tracker.fill_missing_blocks_and_verify()

        return wrapper
    return decorator


class SkvbcWriteRequest:
    """
    A write request sent to an Skvbc cluster. A request may or may not complete.
    """
    def __init__(self, client_id, seq_num, readset, writeset, read_block_id=0):
        self.timestamp = time.monotonic()
        self.client_id = client_id
        self.seq_num = seq_num
        self.readset = readset
        self.writeset = writeset
        self.read_block_id = read_block_id

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  timestamp={self.timestamp}\n'
           f'  client_id={self.client_id}\n'
           f'  seq_num={self.seq_num}\n'
           f'  readset={self.readset}\n'
           f'  writeset={self.writeset}\n'
           f'  read_block_id={self.read_block_id}\n')

class SkvbcReadRequest:
    """
    A read request sent to an Skvbc cluster. A request may or may not complete.
    """
    def __init__(self, client_id, seq_num, readset, read_block_id=0):
        self.timestamp = time.monotonic()
        self.client_id = client_id
        self.seq_num = seq_num
        self.readset = readset
        self.read_block_id = read_block_id

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  timestamp={self.timestamp}\n'
           f'  client_id={self.client_id}\n'
           f'  seq_num={self.seq_num}\n'
           f'  readset={self.readset}\n'
           f'  read_block_id={self.read_block_id}\n')

class SkvbcGetLastBlockReq:
    """
    A GET_LAST_BLOCK request sent to an skvbc cluster. A request may or may not
    complete.
    """
    def __init__(self, client_id, seq_num):
        self.timestamp = time.monotonic()
        self.client_id = client_id
        self.seq_num = seq_num

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  timestamp={self.timestamp}\n'
           f'  client_id={self.client_id}\n'
           f'  seq_num={self.seq_num}\n')

class SkvbcWriteReply:
    """A reply to an outstanding write request sent to an Skvbc cluster."""
    def __init__(self, client_id, seq_num, reply):
        self.timestamp = time.monotonic()
        self.client_id = client_id
        self.seq_num = seq_num
        self.reply = reply

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  timestamp={self.timestamp}\n'
           f'  client_id={self.client_id}\n'
           f'  seq_num={self.seq_num}\n'
           f'  reply={self.reply}\n')

class SkvbcReadReply:
    """A reply to an outstanding read request sent to an Skvbc cluster."""
    def __init__(self, client_id, seq_num, kvpairs):
        self.timestamp = time.monotonic()
        self.client_id = client_id
        self.seq_num = seq_num
        self.kvpairs = kvpairs

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  timestamp={self.timestamp}\n'
           f'  client_id={self.client_id}\n'
           f'  seq_num={self.seq_num}\n'
           f'  kvpairs={self.kvpairs}\n')

class SkvbcGetLastBlockReply:
    """
    A reply to an outstanding get last block request sent to an Skvbc cluster.
    """
    def __init__(self, client_id, seq_num, reply):
        self.timestamp = time.monotonic()
        self.client_id = client_id
        self.seq_num = seq_num
        self.reply = reply

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  timestamp={self.timestamp}\n'
           f'  client_id={self.client_id}\n'
           f'  seq_num={self.seq_num}\n'
           f'  reply={self.reply}\n')

class Result(Enum):
   """
   Whether an operation succeeded, failed, or the result is unknown
   """
   WRITE_SUCCESS = 1
   WRITE_FAIL = 2
   UNKNOWN_WRITE = 3
   UNKNOWN_READ = 4
   READ_REPLY = 5

class CompletedRead:
    def __init__(self, causal_state, kvpairs):
        self.causal_state = causal_state
        self.kvpairs = kvpairs

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  causal_state={self.causal_state}\n'
           f'  kvpairs={self.kvpairs}\n')

class CausalState:
    """Relevant state of the tracker before a request is started"""
    def __init__(self,
                 req_index,
                 last_known_block,
                 last_consecutive_block,
                 missing_intermediate_blocks,
                 kvpairs):
        self.req_index = req_index
        self.last_known_block = last_known_block
        self.last_consecutive_block = last_consecutive_block
        self.missing_intermediate_blocks = missing_intermediate_blocks

        # KV pairs contain the value up keys up until last_consecutive_block
        self.kvpairs = kvpairs

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'    req_index={self.req_index}\n'
           f'    last_known_block={self.last_known_block}\n'
           f'    last_consecutive_block={self.last_consecutive_block}\n'
           f'    '
           f'missing_intermediate_blocks={self.missing_intermediate_blocks}\n'
           f'    causal state kvpairs={self.kvpairs}\n')

class ConcurrentValue:
    """Track the state for a request / reply in self.concurrent"""
    def __init__(self, is_read):
        if is_read:
            self.result = Result.UNKNOWN_READ
        else:
            self.result = Result.UNKNOWN_WRITE

        # Only used in writes
        # Set only when writes succeed.
        self.written_block_id = None

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  result={self.result}\n'
           f'  written_block_id={self.written_block_id}\n')

class Block:
    def __init__(self, kvpairs, req_index=None):
        self.kvpairs = kvpairs
        self.req_index = req_index

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  kvpairs={self.kvpairs}\n'
           f'  req_index={self.req_index}\n')

class Status:
    """
    Status about the order of writes and reads.
    This is useful for debugging linearizability check failures.
    """
    def __init__(self, config):
        self.config = config
        self.start_time = time.monotonic()
        self.end_time = 0
        self.last_client_reply = 0
        self.client_timeouts = {}
        self.client_replies = {}

    def record_client_reply(self, client_id):
        self.last_client_reply = time.monotonic()
        count = self.client_replies.get(client_id, 0)
        self.client_replies[client_id] = count + 1

    def record_client_timeout(self, client_id):
        count = self.client_timeouts.get(client_id, 0)
        self.client_timeouts[client_id] = count + 1

    def __str__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  config={self.config}\n'
           f'  test_duration={self.end_time - self.start_time} seconds\n'
           f'  time_since_last_client_reply='
           f'{self.end_time - self.last_client_reply} seconds\n'
           f'  client_timeouts={self.client_timeouts}\n'
           f'  client_replies={self.client_replies}\n')

class SkvbcTracker:
    """
    Track requests, expected and actual responses from SimpleKVBC test
    clusters, and then allow verification of the linearizability of responses.

    The SkvbcTracker is used by a test to record messages sent and replies
    received to a cluster of SimpleKVBC TesterReplica nodes. Every request
    sent and reply received is recorded in `self.history`, and indexes into this
    history for requesets are frequently referred to by other parts of this
    code.

    By tracking outstanding requests in `self.outstanding` it is able to
    determine which requests are concurrent with which other requests, and track
    them in `self.concurrent`. The tracker also records a set of known blocks in
    `self.blocks` that it learns when ConditionalWrite replies are successful.

    Sometimes replies do not arrive for requests, and therefore the requests are never
    removed from self.outstanding. Such requests are considered concurrent with
    all future requests.

    Additionally, the tracker records all completed reads and failed writes so
    that they can be put into a total order with the blocks and checked for
    correctness (i.e. linearized).

    A test can send many requests to a cluster while it is also crashing nodes,
    creating partitions, instigating view changes, playing with clocks, and
    performing generally malicious actions. These failures can trigger missing
    replies as well as anomalies in the record of blocks themselves. The goal of
    the test is to try to trigger these anomalies via contentious writes and
    failures, such that any flaws in the implementation of concord-sbft can
    revealed when the tracker performs verification. When a failure is found the
    tracker state can be dumped along with the specific exception thrown to help
    developers reason about bugs in the code.

    Any complexity in this code stems primarily from the requirements and
    implementation of the verification procedure. Unlike general, dynamic
    model based linearizability checkers like
    [knossos](https://github.com/jepsen-io/knossos) or
    [porcupine](https://github.com/anishathalye/porcupine), which attempt an
    NP-Hard search for correct linearizations soley from requests and responses,
    the SkvbcTracker relies on the total ordering of blocks provided by the
    TesterReplica implementation, and can perform correctness checks in linear
    time. However, the tester also records the complete history of requests and
    replies and can be used to generate a compatible output file that can then
    be verified using either knossos or porcupine.

    Every time a successful conditional write reply is received it includes the
    block number that it generated. This allows us to create a definitive total
    order for all blocks generated from requests that the tracker knows the
    replies of. However, we don't know the value of all blocks, because some
    replies may have been dropped. Before a test calls the `verify` method, it
    must retrieve and inform the tracker about any missing blocks. However, the
    test doesn't know which blocks are missing. The test will therfore call the
    `get_missing_blocks` method with the last_block_id of the cluster, that it
    retrieves after it is done sending read and write requests. The
    `get_missing_blocks` method will return a set of blocks which the test
    should retrieve from the replicas and then inform the tracker about with a
    call to `fill_missing_blocks`. After `fill_missing_blocks` is called the
    tracker will have a total order of all blocks in the system, thus obviating
    the need to perform an NP-hard search to find a write order that also
    satisfies concurrent reads and failed responses.

    An astute reader will note that we are trusting the TesterReplica cluster to
    tell us about the values of certain blocks, but that cluster is also the
    system under test that we are attempting to validate. At an absolute
    minimum, we want to make sure that any filled blocks could have been
    generated by outstanding requests. Therefore, the first step in verification
    is to call `_match_filled_blocks`. If any blocks could not have been
    generated by the outstanding requests, i.e. requests without replies, than a
    `PhantomBlockError` exception is raised. We could also try to order
    identical requests and blocks and determine if they could have successfully
    created those blocks based on their readsets not conflicting with writesets
    in the block history. However, this is an expensive search, that is probably
    similar to general linearizability testing in its complexity, and in long
    histories with many missing blocks may prolong all tests with little benefit. If we
    determine the need we can add this procedure later. The assumption here is that
    any incorrectness in the missing blocks will be detected via the inability
    of reads and failed requests to serialize, or be the result of unmatched
    requests. In other words, if the missing blocks are incorrect, other parts
    of the chain are likely incorrect and will be detected from the remaining
    parts of the verification procedure.

    The remaining parts of the verification procedure are documented in their
    respective methods, but will briefly be described here.

    `_verify_successful_writes` ensures that the readset in the write request
    does not intersect with the writesets in other requests that created blocks
    up until the creation of the block by this successful write. In essence, it
    is verifying compare and swap behavior, using the block id of the readset as
    a version.

    `_linearize_reads` ensures that the returned values of the read could have
    been returned from the *state* of the kvstore before or after any concurrent
    requests with the read.

    `_linearize_write_failures` works similarly to `_linearize_reads`, in that it
    looks at concurrent write requests to determine if there was an anomaly. The
    anomaly in this case would be if the failed request had a readset that
    didn't actually intersect with any writesets in concurrent requests. This
    would mean that there wasn't actually a conflict, and therefore the write
    should have succeeded. It's important to note here that we are only checking
    explicit failures returned from the cluster that are due to conflict errors
    in the replica's block state. We do not try to show that timeouts should not
    have failed. Any request that times out is not recorded as having a reply
    and remains in `self.outstanding`.

    It's important to note that the verification procedure returns the first
    error it finds in an exception. There may be other errors that were not
    reported but existed. In the future we may wish to change this code to track
    all (or most) errors and report a set of exceptions to the caller, to enable better
    debugging.

    One more check that may be useful is to take the complete chain in
    `self.blocks` and compare it to all blocks in the cluster at the end of the
    test run. The values should be identical. This can be an expensive check in
    clusters with lots of blocks, but we may want to add it as an optional check
    in the future.
    """
    def __init__(self, initial_kvpairs={}, skvbc=None, bft_network=None, pre_exec_all=False, no_conflicts=False, block_Accumulation=False):

        # If this flag is set to True, it means that all the tracker requests will
        # go through the pre_execution mechanism
        self.pre_exec_all = pre_exec_all

        # If set to True, it means that we always sends an empty read set in the bft write request, in order to prevent
        # conflicts
        self.no_conflicts = no_conflicts

        self.block_Accumulation = block_Accumulation
        # A partial order of all requests (SkvbcWriteRequest | SkvbcReadRequest)
        # issued against SimpleKVBC.  History tracks requests and responses. A
        # happens-before relationship exists between responses and requests
        # launched after those responses.
        self.history = []

        # All currently outstanding requests:
        # (client_id, seq_num) -> CausalState
        self.outstanding = {}

        # All completed reads by index into history -> CompletedRead
        self.completed_reads = {}

        # All failed writes by index into history -> CausalState
        self.failed_writes = {}

        # A set of all concurrent requests and their results for each request in
        # history
        # index -> dict{index, ConcurrentValue}
        self.concurrent = {}

        # All blocks and their kv data based on responses
        # Each known block is mapped from block id to a Block
        self.blocks = {}

        # The value of all keys at last_consecutive_block
        self.kvpairs = initial_kvpairs
        self.last_consecutive_block = 0

        # The block last received in a write
        self.last_known_block = 0

        # Blocks that get filled in by the call to fill_missing_blocks
        # These blocks were created by write requests that never got replies.
        self.filled_blocks = {}

        self.skvbc = skvbc

        self.bft_network = bft_network

        if self.bft_network is not None:
            self.status = Status(bft_network.config)

    def send_write(self, client_id, seq_num, readset, writeset, read_block_id):
        """Track the send of a write request"""
        req = SkvbcWriteRequest(
                client_id, seq_num, readset, writeset, read_block_id)
        self._send_req(req, is_read=False)

    def send_read(self, client_id, seq_num, readset):
        """
        Track the send of a read request.

        Always get the latest value. We are trying to linearize requests, so we
        want a real-time ordering which requires getting the latest values.
        """
        req = SkvbcReadRequest(client_id, seq_num, readset)
        self._send_req(req, is_read=True)

    def _send_req(self, req, is_read):
        self.history.append(req)
        index = len(self.history) - 1
        self._update_concurrent_requests(index, is_read)
        cs = CausalState(index,
                         self.last_known_block,
                         self.last_consecutive_block,
                         self._count_non_consecutive_blocks(),
                         self.kvpairs.copy())
        self.outstanding[(req.client_id, req.seq_num)] = cs

    def handle_write_reply(self, client_id, seq_num, reply):
        """
        Match a write reply with its outstanding request.
        Check for consistency violations and raise an exception if found.
        """
        rpy = SkvbcWriteReply(client_id, seq_num, reply)
        self.history.append(rpy)
        req, req_index = self._get_matching_request(rpy)
        if reply.success:
            if (self.block_Accumulation is False and reply.last_block_id in self.blocks):
                # This block_id has already been written!
                block = self.blocks[reply.last_block_id]
                raise ConflictingBlockWriteError(reply.last_block_id, block, req)
            else:
                self._record_concurrent_write_success(req_index,
                                                      rpy,
                                                      reply.last_block_id)
                self.blocks[reply.last_block_id] = Block(req.writeset, req_index)
                if reply.last_block_id > self.last_known_block:
                    self.last_known_block = reply.last_block_id

                # Update consecutive kvpairs
                if reply.last_block_id == self.last_consecutive_block + 1:
                    self.last_consecutive_block += 1
                    self.kvpairs.update(req.writeset)
                    # Did we already have the next consecutive blocks?
                    while True:
                        block = self.blocks.get(self.last_consecutive_block + 1)
                        if block is None:
                            break
                        self.last_consecutive_block += 1
                        self.kvpairs.update(block.kvpairs)
        else:
            self._record_concurrent_write_failure(req_index, rpy)

    def handle_read_reply(self, client_id, seq_num, kvpairs):
        """
        Get a read reply and ensure that it linearizes with the current known
        concurrent replies.
        """
        rpy = SkvbcReadReply(client_id, seq_num, kvpairs)
        req, req_index = self._get_matching_request(rpy)
        self.history.append(rpy)
        self._record_read_reply(req_index, rpy)

    def get_missing_blocks(self, last_block_id):
        """
        Retrieve the set of missing blocks.

        This is called during verification so that the test can retrieve these
        blocks and call self.fill_in_missing_blocks().

        After missing blocks are filled in, successful reads can be linearized.
        """
        missing_blocks = set()
        for i in range(self.last_consecutive_block + 1, self.last_known_block):
            if self.blocks.get(i) is None:
                missing_blocks.add(i)

        # Include last_block_id if not already known
        for i in range(self.last_known_block + 1, last_block_id + 1):
            missing_blocks.add(i)

        log.log_message(message_type=f'{len(missing_blocks)} missing blocks found.')
        return missing_blocks

    def fill_missing_blocks(self, missing_blocks):
        """
        Add all missing blocks to self.blocks

        Note that these blocks will not have a matching req_index since we never
        received a reply for the request that created it. In some histories it's
        not possible to identify an unambiguous request, since there may be
        multiple possible requests that could have correctly generated the
        block. Rather than trying to match the requests, to the missing blocks,
        we just assume the missing blocks are correct for now, and use the full
        block history to verify successful conditional writes and reads.
        """
        for block_id, kvpairs in missing_blocks.items():
            self.blocks[block_id] = Block(kvpairs)
            if block_id > self.last_known_block:
                self.last_known_block = block_id
        self.filled_blocks = missing_blocks
        log.log_message(message_type=f'{len(missing_blocks)} missing blocks filled.')

    def verify(self):
        self._match_filled_blocks()
        self._verify_successful_writes()
        self._linearize_reads()
        self._linearize_write_failures()

    def _match_filled_blocks(self):
        """
        For every filled block, identify an outstanding write request with a
        matching writeset.

        If there isn't an outstanding request that could have generated the
        block raise a PhantomBlockError.
        """

        # Req/block_id pairs
        matched_blocks = []
        write_requests = self._get_all_outstanding_write_requests()

        for block_id, block_kvpairs in self.filled_blocks.items():
            unmatched = []
            success = False
            for _ in range(0, len(write_requests)):
                req = write_requests.pop()
                if req.writeset == block_kvpairs:
                    matched_blocks.append((req, block_id))
                    success = True
                    break
                else:
                    unmatched.append(req)
            if not success:
                raise PhantomBlockError(block_id,
                                        block_kvpairs,
                                        matched_blocks,
                                        unmatched)
            write_requests.extend(unmatched)

    def _get_all_outstanding_write_requests(self):
        writes = []
        for causal_state in self.outstanding.values():
            req = self.history[causal_state.req_index]
            if isinstance(req, SkvbcWriteRequest):
                writes.append(req)
        return writes

    def _verify_successful_writes(self):
        for i in range(1, self.last_known_block+1):
            req_index = self.blocks[i].req_index
            if req_index != None:
                # A reply was received for this request that created the block
                req = self.history[req_index]
                self._verify_successful_write(i, req)

    def _linearize_write_failures(self):
        """
        Go through all write failures, and determine if they should have failed
        due to conflict.

        The failure check involves looking for write conflicts based on the
        causal state at the time the failed request was issued, and any
        succeeding concurrent writes.

        If no concurrent writes had writesets that intersected with the readset
        of the failed write, then that write should have succeeded. In this case
        we raise a NoConflictError.

        Note that we only count failures explicitly returned from skvbc, i.e.
        those where writes returned success = false due to conflict. Timeouts
        remain in outstanding requests, since we don't know whether they would
        have succeeded or failed.
        """
        for req_index, causal_state in self.failed_writes.items():
            blocks_to_check = self._num_blocks_to_linearize_over(req_index,
                                                                 causal_state)
            failed_req = self.history[req_index]

            # Check for writeset intersection at every block from the block
            # after the readset until the last possible concurrently generated
            # block.
            success = False
            for i in range(failed_req.read_block_id + 1,
                           causal_state.last_known_block + blocks_to_check + 1):
                writeset = set(self.blocks[i].kvpairs.keys())
                if len(failed_req.readset.intersection(writeset)) != 0:
                       # We found a block that conflicts. We must
                       # assume that failed_req was failed correctly.
                       success = True
                       break

            if not success:
                # We didn't find any conflicting blocks.
                # failed_req should have succeeded!
                raise NoConflictError(failed_req, causal_state)

    def _linearize_reads(self):
        """
        At this point, we should know the kv pairs of all blocks.

        Attempt to find linearization points for all reads in
        self.completed_reads.

        If a read cannot be linearized, then raise an exception.
        """
        for req_index, completed_read  in self.completed_reads.items():
            cs = completed_read.causal_state
            kv = cs.kvpairs.copy()

            # We must check that the read linearizes after
            # causal_state.last_known_block, since it must have started after
            # that. Build up the kv state until last_known_block.
            for block_id in range(cs.last_consecutive_block + 1,
                                  cs.last_known_block + 1):
                kv.update(self.blocks[block_id].kvpairs)

            blocks_to_check = self._num_blocks_to_linearize_over(req_index, cs)
            success = False
            for i in range(cs.last_known_block,
                           cs.last_known_block + blocks_to_check + 1):
                if i != cs.last_known_block:
                    kv.update(self.blocks[i].kvpairs)
                if self._read_is_valid(kv, completed_read.kvpairs):
                    # The read linearizes here
                    success = True
                    break

            if not success:
                raise InvalidReadError(completed_read,
                                       self.concurrent[req_index])

    def _num_blocks_to_linearize_over(self, req_index, causal_state):
        """
        The number of blocks after causal_sate.last_known_block that we must
        iterate over fo see if a read or write failure can linearize after each
        of the blocks.
        """
        cs = causal_state
        max_concurrent = self._max_possible_concurrent_writes(req_index)
        concurrent_remaining = max_concurrent - cs.missing_intermediate_blocks
        total_remaining = self.last_known_block - cs.last_known_block
        return min(concurrent_remaining, total_remaining)

    def _read_is_valid(self, kv_state, read_kvpairs):
        """Return if a read of read_kvpairs is possible given kv_state."""
        for k, v in read_kvpairs.items():
            if kv_state.get(k) != v:
                return False
        return True

    def _max_possible_concurrent_writes(self, req_index):
        """
        Return the maximum possible number of concurrrent writes.
        This includes writes that returned successfully but also writes with no
        return that could have generated missing blocks.
        """
        count = 0
        for val in self.concurrent[req_index].values():
            if val.result == Result.WRITE_SUCCESS or \
               val.result == Result.UNKNOWN_WRITE:
                   count += 1
        return count

    def _update_concurrent_requests(self, index, is_read):
        """
        Set the concurrent requests for this request to a dictionary of all
        indexes into history in self.outstanding mapped to a ConcurrentValue.

        Also update all concurrent requests to include this request in their
        concurrent dicts.
        """
        self.concurrent[index] = {}
        for causal_state in self.outstanding.values():
            i = causal_state.req_index
            is_read_outstanding = isinstance(self.history[i], SkvbcReadRequest)
            # Add the outstanding request to this request's concurrent dicts
            self.concurrent[index][i] = ConcurrentValue(is_read_outstanding)
            # Add this request to the concurrent dicts of each outstanding req
            self.concurrent[i][index] = ConcurrentValue(is_read)

    def _verify_successful_write(self, written_block_id, req):
        """
        Ensure that the block at written_block_id should have been written by
        req.

        Check that for each key in the readset, there have been no writes to
        those keys for each block after the block version in the conditional
        write up to, but not including this block. An example of failure is:

          * We read block id = X
          * We write block id = X + 2
          * We notice that block id X + 1 has written a key in the readset of
            this request that created block X + 2.

        Note that we may have unknown blocks due to missing responses.  We just
        skip these blocks, as we can't tell if there's a conflict or not. We
        have to assume there isn't a conflict in this case.

        If there is a conflicting block then there is a bug in the consensus
        algorithm, and we raise a StaleReadError.
        """
        for i in range(req.read_block_id + 1, written_block_id):
            if i not in self.blocks:
                # Ensure we have learned about this block.
                # Move on if we have not.
                continue
            block = self.blocks[i]

            # If the writeset of the request that created intermediate blocks
            # intersects the readset of this request, then we have a conflict.
            if len(req.readset.intersection(set(block.kvpairs.keys()))) != 0:
                raise StaleReadError(req.read_block_id, i, written_block_id)

    def _record_concurrent_write_success(self, req_index, rpy, block_id):
        """Inform all concurrent requests that this request succeeded."""
        # We don't need the causal state for verification on write successes
        del self.outstanding[(rpy.client_id, rpy.seq_num)]

        val = ConcurrentValue(is_read=False)
        val.result = Result.WRITE_SUCCESS
        val.written_block_id = block_id
        for i in self.concurrent[req_index].keys():
            self.concurrent[i][req_index] = val

    def _record_concurrent_write_failure(self, req_index, rpy):
        """Inform all concurrent requests that this request failed."""
        causal_state = self.outstanding.pop((rpy.client_id, rpy.seq_num))
        self.failed_writes[req_index] = causal_state

        val = ConcurrentValue(is_read=False)
        val.result = Result.WRITE_FAIL
        for i in self.concurrent[req_index].keys():
            self.concurrent[i][req_index] = val

    def _record_read_reply(self, req_index, rpy):
        """Inform all concurrent requests about a read reply"""
        causal_state = self.outstanding.pop((rpy.client_id, rpy.seq_num))
        self.completed_reads[req_index] = CompletedRead(causal_state,
                                                        rpy.kvpairs)

        for i in self.concurrent[req_index].keys():
            self.concurrent[i][req_index].result = Result.READ_REPLY

    def _count_non_consecutive_blocks(self):
        """
        Count the number of missing blocks between self.last_consecutive_block
        to self.last_known_block.
        """
        count = 0
        for i in range(self.last_consecutive_block + 1, self.last_known_block):
            if i not in self.blocks:
                count += 1
        return count

    def _get_matching_request(self, rpy):
        """
        Return the request that matches rpy along with its index into
        self.history.
        """
        causal_state = self.outstanding[(rpy.client_id, rpy.seq_num)]
        index = causal_state.req_index
        return (self.history[index], index)

    async def fill_missing_blocks_and_verify(self):
       try:
           # Use a new client, since other clients may not be responsive due to
           # past failed responses.
           client = self.skvbc.bft_network.new_reserved_client()
           last_block_id = await self.get_last_block_id(client)
           print(f'Last Block ID = {last_block_id}')
           missing_block_ids = self.get_missing_blocks(last_block_id)
           print(f'Missing Block IDs = {missing_block_ids}')
           blocks = await self.get_blocks(client, missing_block_ids)
           self.fill_missing_blocks(blocks)
           self.verify()
       except Exception as e:
           print(f'retries = {client.retries}')
           self.status.end_time = time.monotonic()
           print("HISTORY...")
           for i, entry in enumerate(self.history):
               print(f'Index = {i}: {entry}\n')
           print("BLOCKS...")
           print(f'{self.blocks}\n')
           print(str(self.status), flush=True)
           print("FAILURE...")
           raise (e)

    async def get_blocks(self, client, block_ids):
        blocks = {}
        for block_id in block_ids:
            retries = 12 # 60 seconds
            for i in range(0, retries):
                try:
                    msg = kvbc.SimpleKVBCProtocol.get_block_data_req(block_id)
                    blocks[block_id] = kvbc.SimpleKVBCProtocol.parse_reply(await client.read(msg))
                    break
                except trio.TooSlowError:
                    if i == retries - 1:
                        raise
            log.log_message(message_type=f'Retrieved block {block_id}')
        return blocks

    async def get_last_block_id(self, client):
        msg = kvbc.SimpleKVBCProtocol.get_last_block_req()
        return kvbc.SimpleKVBCProtocol.parse_reply(await client.read(msg))
        
    def read_block_id(self):
        start = max(0, self.last_known_block - MAX_LOOKBACK)
        return random.randint(start, self.last_known_block)
    
