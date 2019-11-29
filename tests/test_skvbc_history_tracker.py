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

import unittest
from util import skvbc, skvbc_history_tracker

from util.skvbc_exceptions import(
    ConflictingBlockWriteError,
    StaleReadError,
    NoConflictError,
    InvalidReadError,
    PhantomBlockError
)

class TestCompleteHistories(unittest.TestCase):
    """Test histories where no blocks are unknown"""

    def setUp(self):
        self.tracker = skvbc_history_tracker.SkvbcTracker()

    def test_sucessful_write(self):
        """
        A single request results in a single successful reply.
        The checker finds no errors.
        """
        client_id = 0
        seq_num = 0
        readset = set()
        read_block_id = 0
        writeset = {'a': 'a'}
        self.tracker.send_write(
                client_id, seq_num, readset, writeset, read_block_id)
        reply = skvbc.WriteReply(success=True, last_block_id=1)
        self.tracker.handle_write_reply(client_id, seq_num, reply)
        self.tracker.verify()

    def test_failed_write(self):
        """
        A single request results in a single failed reply.
        The checker finds a NoConflictError, because there were no concurrent
        requests that would have caused the request to fail explicitly.
        """
        client_id = 0
        seq_num = 0
        readset = set()
        read_block_id = 0
        writeset = {'a': 'a'}
        self.tracker.send_write(
                client_id, seq_num, readset, writeset, read_block_id)
        reply = skvbc.WriteReply(success=False, last_block_id=1)
        self.tracker.handle_write_reply(client_id, seq_num, reply)

        with self.assertRaises(NoConflictError) as err:
            self.tracker.verify()

        self.assertEqual(0, err.exception.causal_state.req_index)

    def test_contentious_writes_one_success_one_fail(self):
        """
        Two writes contend for the same key. Only one should succeed.
        The checker should not see an error.
        """
        # First create an initial block so we can contend with it
        self.test_sucessful_write()
        # Send 2 concurrent writes with the same readset and writeset
        readset = set("a")
        writeset = {'a': 'a'}
        self.tracker.send_write(0, 1, readset, writeset, 1)
        self.tracker.send_write(1, 1, readset, writeset, 1)
        self.tracker.handle_write_reply(0, 1, skvbc.WriteReply(False, 0))
        self.tracker.handle_write_reply(1, 1, skvbc.WriteReply(True, 2))
        self.tracker.verify()

    def test_contentious_writes_both_succeed(self):
        """
        Two writes contend for the same key. Both succeed.
        The checker should raise a StaleReadError, since only one should have
        succeeded.
        """
        # First create an initial block so we can contend with it
        self.test_sucessful_write()
        # Send 2 concurrent writes with the same readset and writeset
        readset = set("a")
        writeset = {'a': 'a'}
        self.tracker.send_write(0, 1, readset, writeset, 1)
        self.tracker.send_write(1, 1, readset, writeset, 1)
        self.tracker.handle_write_reply(1, 1, skvbc.WriteReply(True, 2))
        self.tracker.handle_write_reply(0, 1, skvbc.WriteReply(True, 3))

        with self.assertRaises(StaleReadError) as err:
            self.tracker.verify()

        # Check the exception detected the error correctly
        self.assertEqual(err.exception.readset_block_id, 1)
        self.assertEqual(err.exception.block_with_conflicting_writeset, 2)
        self.assertEqual(err.exception.block_being_checked, 3)

    def test_contentious_writes_both_succeed_same_block(self):
        """
        Two writes contend for the same key. Both succeed, apparently
        creating the same block.

        The checker should raise a ConflictingBlockWriteError, since two
        requests cannot create the same block.
        """
        # First create an initial block so we can contend with it
        self.test_sucessful_write()
        # Send 2 concurrent writes with the same readset and writeset
        readset = set("a")
        writeset = {'a': 'a'}
        self.tracker.send_write(0, 1, readset, writeset, 1)
        self.tracker.send_write(1, 1, readset, writeset, 1)
        self.tracker.handle_write_reply(1, 1, skvbc.WriteReply(True, 2))

        with self.assertRaises(ConflictingBlockWriteError) as err:
            self.tracker.handle_write_reply(0, 1, skvbc.WriteReply(True, 2))

        # The conflicting block id was block 2
        self.assertEqual(err.exception.block_id, 2)

    def test_read_between_2_successful_writes(self):
        """
        Send 2 concurrent writes that don't conflict, but overwrite the same
        value, along with a concurrent read. The read sees the first overwrite,
        and should linearize after the first overwritten value correctly.
        """
        # A non-concurrent write
        self.test_sucessful_write()
        writeset_1 = {'a': 'b'}
        writeset_2 = {'a': 'c'}
        read_block_id = 1
        client0 = 0
        client1 = 1
        client2 = 2

        # 2 concurrent writes and a concurrent read
        self.tracker.send_write(client0, 1, set(), writeset_1, read_block_id)
        self.tracker.send_write(client1, 1, set(), writeset_2, read_block_id)
        self.tracker.send_read(client2, 1, ["a"])

        # block2/ writeset_2
        self.tracker.handle_write_reply(client1, 1, skvbc.WriteReply(True, 2))
        # block3/ writeset_1
        self.tracker.handle_write_reply(client0, 1, skvbc.WriteReply(True, 3))
        self.tracker.handle_read_reply(client2, 1, {"a":"b"})
        self.tracker.verify()

    def test_read_does_not_linearize_no_concurrent_writes(self):
        """
        Read a value that doesn't exist.
        This should raise an exception.
        """
        # state = {'a':'a'}
        self.test_sucessful_write()

        self.tracker.send_read(1, 0, ["a"])
        self.tracker.handle_read_reply(1, 0, {"a":"b"})
        with self.assertRaises(InvalidReadError) as err:
            self.tracker.verify()

        self.assertEqual(0, len(err.exception.concurrent_requests))

    def test_read_does_not_linearize_two_concurrent_writes(self):
        """
        Read a value that doesn't exist when concurrent with 2 reads.
        This should raise an exception.
        """
        self.test_sucessful_write()
        writeset_1 = {'a': 'b'}
        writeset_2 = {'a': 'c'}
        read_block_id = 1
        client0 = 0
        client1 = 1
        client2 = 2

        # 2 concurrent writes and a concurrent read
        self.tracker.send_write(client0, 1, set(), writeset_1, read_block_id)
        self.tracker.send_write(client1, 1, set(), writeset_2, read_block_id)
        self.tracker.send_read(client2, 1, ["a"])

        # block2/ writeset_2
        self.tracker.handle_write_reply(client1, 1, skvbc.WriteReply(True, 2))
        # block3/ writeset_1
        self.tracker.handle_write_reply(client0, 1, skvbc.WriteReply(True, 3))

        # Read a value that was never written
        self.tracker.handle_read_reply(client2, 1, {"a":"d"})

        with self.assertRaises(InvalidReadError) as err:
            self.tracker.verify()

        self.assertEqual(2, len(err.exception.concurrent_requests))

    def test_read_is_stale_with_concurrent_writes(self):
        """
        Read a value that doesn't exist when concurrent with 2 reads.
        This should raise an exception.
        """
        self.test_sucessful_write()
        writeset_1 = {'a': 'b'}
        writeset_2 = {'a': 'c'}
        read_block_id = 1
        client0 = 0
        client1 = 1
        client2 = 2

        # Another successful write commits.
        # All subsequent ops should see at least state {'a':'b'}
        self.tracker.send_write(client0, 1, set(), writeset_1, read_block_id)
        self.tracker.handle_write_reply(client0, 1, skvbc.WriteReply(True, 2))

        # A concurrent write and a concurrent read
        self.tracker.send_write(client1, 1, set(), writeset_2, read_block_id)
        self.tracker.send_read(client2, 1, ["a"])

        # block3/ writeset_2
        self.tracker.handle_write_reply(client1, 1, skvbc.WriteReply(True, 3))

        # Read a stale value. Key "a" was updated to "b" before the read was
        # sent. "a" can only be "b" or "c".
        self.tracker.handle_read_reply(client2, 1, {"a":"a"})

        with self.assertRaises(InvalidReadError) as err:
            self.tracker.verify()

        self.assertEqual(1, len(err.exception.concurrent_requests))

    def test_long_correct_history(self):
        """
        Launch a batch of unconditional writes along with reads concurrently
        a few times. The responses should always be correct.
        """
        self.test_sucessful_write()
        writeset_1 = {'a': 'b'}
        writeset_2 = {'a': 'c'}
        read_block_id = 1

        written_block_id = 1;
        for seq_num in range(0, 5):

            # track requests
            for client_id in range(0, 20):
                if client_id % 3 == 0:
                    self.tracker.send_read(client_id, seq_num, ["a"])
                else:
                    w = writeset_1
                    if client_id % 2 == 0:
                        w = writeset_2
                    self.tracker.send_write(
                        client_id, seq_num, set(), w, read_block_id)

            # track replies
            for client_id in range(0, 20):
                if client_id % 3 == 0:
                    # expected doesn't really matter for correctness
                    # it just has to be one of the two written values because
                    # all writes and reads in a batch are concurrent.
                    expected = writeset_2
                    if client_id % 2 == 0:
                        expected = writeset_1
                    self.tracker.handle_read_reply(client_id, seq_num, expected)
                else:
                     written_block_id += 1;
                     reply = skvbc.WriteReply(True, written_block_id)
                     self.tracker.handle_write_reply(client_id, seq_num, reply)

        self.tracker.verify()

    def test_long_failed_history(self):
        """
        Launch a batch of unconditional writes along with reads concurrently
        a few times. Insert a single incorrect read reply.
        """
        self.test_sucessful_write()
        writeset_1 = {'a': 'b'}
        writeset_2 = {'a': 'c'}
        read_block_id = 1
        stale_read = {'a': 'a'}

        written_block_id = 1;
        for seq_num in range(0, 5):

            # track requests
            for client_id in range(0, 20):
                if client_id % 3 == 0:
                    self.tracker.send_read(client_id, seq_num, ["a"])
                else:
                    w = writeset_1
                    if client_id % 2 == 0:
                        w = writeset_2
                    self.tracker.send_write(
                        client_id, seq_num, set(), w, read_block_id)

            # track replies
            for client_id in range(0, 20):
                if client_id % 3 == 0:
                    # expected doesn't really matter for correctness
                    # it just has to be one of the two written values because
                    # all writes and reads in a batch are concurrent.
                    expected = writeset_2
                    if client_id % 2 == 0:
                        expected = writeset_1
                    if client_id == 6 and seq_num == 3:
                        self.tracker.handle_read_reply(
                                client_id, seq_num, stale_read)
                    else:
                        self.tracker.handle_read_reply(client_id, seq_num, expected)
                else:
                     written_block_id += 1;
                     reply = skvbc.WriteReply(True, written_block_id)
                     self.tracker.handle_write_reply(client_id, seq_num, reply)

        with self.assertRaises(InvalidReadError) as err:
            self.tracker.verify()

        self.assertEqual(19, len(err.exception.concurrent_requests))
        self.assertEqual(stale_read, err.exception.read.kvpairs)



class TestPartialHistories(unittest.TestCase):
    """
    Test histories where some writes don't return replies.

    In these cases, blocks may be generated from the write, but the tracker
    doesn't know what the values are until after the test iteration, when the
    tester informs it of the missing blocks values.
    """
    def setUp(self):
        self.tracker = skvbc_history_tracker.SkvbcTracker()

    def test_sucessful_write(self):
        """
        A single request results in a single successful reply.
        The checker finds no errors.
        """
        client_id = 0
        seq_num = 0
        readset = set()
        read_block_id = 0
        writeset = {'a': 'a'}
        self.tracker.send_write(
                client_id, seq_num, readset, writeset, read_block_id)
        reply = skvbc.WriteReply(success=True, last_block_id=1)
        self.tracker.handle_write_reply(client_id, seq_num, reply)
        self.tracker.verify()

    def test_2_concurrent_writes_one_missing_success_two_concurrent_reads(self):
        """
        Two concurrent writes are sent with two concurrent reads. One write does
        not respond.

        One read should linearize correctly based on the write that returned,
        and the other based on the one that didn't return.
        """
        # A non-concurrent write
        self.test_sucessful_write()
        writeset_1 = {'a': 'b'}
        writeset_2 = {'a': 'c'}
        read_block_id = 1
        client0 = 0
        client1 = 1
        client2 = 2
        client3 = 3

        # 2 concurrent unconditional writes and a concurrent read
        self.tracker.send_write(client0, 1, set(), writeset_1, read_block_id)
        self.tracker.send_write(client1, 1, set(), writeset_2, read_block_id)
        self.tracker.send_read(client2, 1, ["a"])
        self.tracker.send_read(client3, 1, ["a"])

        # writeset_2 written at block 3
        self.tracker.handle_write_reply(client1, 1, skvbc.WriteReply(True, 3))
        self.tracker.handle_read_reply(client2, 1, writeset_1)
        self.tracker.handle_read_reply(client3, 1, writeset_2)

        self.assertEqual(3, self.tracker.last_known_block)

        # The test should call these methods when it stops sending
        # operations.
        # The test must ask the tracker what blocks are missing
        missing_block_id = 2
        last_block = 3
        missing_blocks = self.tracker.get_missing_blocks(last_block)
        self.assertSetEqual(set([missing_block_id]), missing_blocks)

        self.assertEqual(3, self.tracker.last_known_block)

        # The test must retrieve the missing blocks from skvbc TesterReplicas
        # using the clients and them inform the tracker. This is the call that
        # informs the tracker.
        self.tracker.fill_missing_blocks({missing_block_id: writeset_1})

        # The tracker now has all the information it needs. Tell it to verify
        # that it's read responses linearize.
        self.tracker.verify()

    def test_2_concurrent_writes_one_missing_failure_two_concurrent_reads(self):
        """
        Two concurrent writes are sent with two concurrent reads. One write does
        not respond.

        One read should linearize correctly based on the write that returned,
        and the other based on the one that didn't return.
        """
        # A non-concurrent write
        self.test_sucessful_write()
        writeset_1 = {'a': 'b'}
        writeset_2 = {'a': 'c'}
        read_block_id = 1
        client0 = 0
        client1 = 1
        client2 = 2
        client3 = 3

        # 2 concurrent unconditional writes and a concurrent read
        self.tracker.send_write(client0, 1, set("a"), writeset_1, read_block_id)
        self.tracker.send_write(client1, 1, set("a"), writeset_2, read_block_id)
        self.tracker.send_read(client2, 1, ["a"])
        self.tracker.send_read(client3, 1, ["a"])

        # writeset_2 written at block 3
        self.tracker.handle_write_reply(client1, 1, skvbc.WriteReply(True, 2))
        self.tracker.handle_read_reply(client2, 1, writeset_1)
        self.tracker.handle_read_reply(client3, 1, writeset_2)

        self.assertEqual(2, self.tracker.last_known_block)

        # the last block is 2 because one of the writes conflicted
        last_block = 2
        missing_blocks = self.tracker.get_missing_blocks(last_block)
        self.assertSetEqual(set(), missing_blocks)

        with self.assertRaises(InvalidReadError) as err:
            self.tracker.verify()

        self.assertEqual(3, len(err.exception.concurrent_requests))
        # The read of writeset_1 fails, since that was the failed request that
        # never returned.
        self.assertEqual(writeset_1, err.exception.read.kvpairs)

    def test_phantom_block_fill_hole(self):
        """
        Create a phantom block that fills in a missing block in the middle of
        the block chain, and ensure a PhantomBlockError gets raised.
        """
        self.test_sucessful_write()
        writeset_1 = {'a': 'b'}
        writeset_2 = {'a': 'c'}
        phantom_writeset = {'a': 'd'}
        read_block_id = 1
        client0 = 0
        client1 = 1

        self.tracker.send_write(client0, 1, set("a"), writeset_1, read_block_id)
        self.tracker.send_write(client1, 1, set("a"), writeset_2, read_block_id)
        self.tracker.handle_write_reply(client1, 1, skvbc.WriteReply(True, 3))

        # We are missing block 2
        last_block = 3
        missing_block_id = 2
        missing_blocks = self.tracker.get_missing_blocks(last_block)
        self.assertSetEqual(set([missing_block_id]), missing_blocks)

        # Fill in block 2 with a block that could never have been generated by a
        # write request
        self.tracker.fill_missing_blocks({missing_block_id: phantom_writeset})

        with self.assertRaises(PhantomBlockError) as err:
            self.tracker.verify()

        self.assertEqual(missing_block_id, err.exception.block_id)
        self.assertEqual(phantom_writeset, err.exception.kvpairs)
        self.assertEqual(0, len(err.exception.matched_blocks))
        self.assertEqual(1, len(err.exception.unmatched_requests))

    def test_phantom_block_fill_extra_block(self):
        """
        Create a phantom block that fills in a block at the end of the block
        chain with an already used request, and ensure a PhantomBlockError gets
        raised. All write_requests matched in this case, but we ran out of them,
        as there was 1 more block than write request.

        """
        self.test_sucessful_write()
        writeset_1 = {'a': 'b'}
        writeset_2 = {'a': 'c'}
        read_block_id = 1
        client0 = 0
        client1 = 1

        self.tracker.send_write(client0, 1, set("a"), writeset_1, read_block_id)
        self.tracker.send_write(client1, 1, set("a"), writeset_2, read_block_id)
        self.tracker.handle_write_reply(client1, 1, skvbc.WriteReply(True, 3))

        # We are missing blocks 2 and 4
        last_block = 4
        missing_block_ids = [2, 4]
        missing_blocks = self.tracker.get_missing_blocks(last_block)
        self.assertSetEqual(set(missing_block_ids), missing_blocks)

        # Fill in a block correctly matching writeset1, and then an extra block
        # with no matching writeset, since we already matched the last request
        # that used writeset_1.
        blocks = {2: writeset_1, 4: writeset_1}
        self.tracker.fill_missing_blocks(blocks)

        with self.assertRaises(PhantomBlockError) as err:
            self.tracker.verify()

        self.assertEqual(4, err.exception.block_id)
        self.assertEqual(writeset_1, err.exception.kvpairs)
        self.assertEqual(1, len(err.exception.matched_blocks))
        self.assertEqual(0, len(err.exception.unmatched_requests))

class TestUnit(unittest.TestCase):

    def test_num_blocks_to_linearize_over(self):
        """
        Create a tracker with enough information to allow running
        _num_blocks_to_linearize_over. Verify that the correct number of blocks
        is returned.
        """
        req_index = 14
        last_known_block = 6
        last_consecutive_block = 3
        missing_intermediate_blocks = 2
        kvpairs = {'a': 1, 'b': 2, 'c': 3}
        cs = skvbc_history_tracker.CausalState(req_index,
                                               last_known_block,
                                               last_consecutive_block,
                                               missing_intermediate_blocks,
                                               kvpairs)

        is_read = False
        write = skvbc_history_tracker.ConcurrentValue(is_read)
        tracker = skvbc_history_tracker.SkvbcTracker()
        tracker.concurrent[req_index] = \
            {2: write, 4: write, 7: write, 11: write, 16: write}

        tracker.last_known_block = 7
        self.assertEqual(1,
                        tracker._num_blocks_to_linearize_over(req_index, cs))

        tracker.last_known_block = 8
        self.assertEqual(2,
                         tracker._num_blocks_to_linearize_over(req_index, cs))

        tracker.last_known_block = 9
        self.assertEqual(3,
                         tracker._num_blocks_to_linearize_over(req_index, cs))

        tracker.last_known_block = 10
        self.assertEqual(3,
                         tracker._num_blocks_to_linearize_over(req_index, cs))

        tracker.last_known_block = 11
        self.assertEqual(3,
                         tracker._num_blocks_to_linearize_over(req_index, cs))

if __name__ == '__main__':
    unittest.main()

