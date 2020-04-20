# Concord
#
# Copyright (c) 2020 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.
import os.path
import unittest
from os import environ

import trio

from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX


def build_cmd(reply_size=None, page_size=None, num_pages=None):
    def start_replica_cmd(builddir, replica_id):
        """
        Return a command that starts an skvbc replica when passed to
        subprocess.Popen.
        """
        statusTimerMilli = "500"
        path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
        cmd = [
          path,
          "-k", KEY_FILE_PREFIX,
          "-i", str(replica_id),
          "-s", statusTimerMilli,
        ]
        if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower() in set(["true", "on"]):
          cmd.append("-p")

        if reply_size is not None:
          cmd.extend(["--max-reply-message-size", str(reply_size)])
        if page_size is not None:
          cmd.extend(["--max-num-of-reserved-pages", str(num_pages)])
        if page_size is not None:
          cmd.extend(["--size-of-reserved-page", str(page_size)])

        return cmd
    return start_replica_cmd

class SkvbcStReplySize(unittest.TestCase):

    __test__ = False  # so that PyTest ignores this test scenario

    async def state_transfer_after_first_checkpoint(self, bft_network):
        """
        Test that state transfer completes for a delayed node.

        Start 3/4 nodes, wait for the first checkpoint to happen. Start the
        remaining node and make sure that state transfer completes.
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)

        late_node = 3
        initial_nodes = bft_network.all_replicas(without={late_node})
        bft_network.start_replicas(initial_nodes)

        await skvbc.fill_and_wait_for_checkpoint(
            initial_nodes,
            checkpoint_num=1,
            verify_checkpoint_persistency=False
        )
        checkpoint = await bft_network.wait_for_checkpoint(replica_id=0)
        self.assertEqual(checkpoint, 1)

        bft_network.start_replica(late_node)
        await bft_network.wait_for_state_transfer_to_start()
        await bft_network.wait_for_state_transfer_to_stop(0, late_node)

    # Note, we only run 4 skvbc test replicas. Each replica is configured to
    # accomodate up to 100 client proxies and hence the formula for maximum
    # number of reserved pages becomes: 100 * 4 * reply_size / page_size

    @with_trio
    @with_bft_network(start_replica_cmd=build_cmd(reply_size=1<<13,
                                                  page_size=1<<12,
                                                  num_pages=800),
                      selected_configs=lambda n,f,c: n==4 and f==1 and c==0)
    async def test_st_after_first_checkpoint_8kb(self, bft_network):
        await self.state_transfer_after_first_checkpoint(bft_network)

    @with_trio
    @with_bft_network(start_replica_cmd=build_cmd(reply_size=1<<14,
                                                  page_size=1<<12,
                                                  num_pages=1600),
                      selected_configs=lambda n,f,c: n==4 and f==1 and c==0)
    async def test_st_after_first_checkpoint_16kb(self, bft_network):
        await self.state_transfer_after_first_checkpoint(bft_network)

    @with_trio
    @with_bft_network(start_replica_cmd=build_cmd(reply_size=1<<20,
                                                  page_size=1<<12,
                                                  num_pages=102400),
                      selected_configs=lambda n,f,c: n==4 and f==1 and c==0)
    async def test_st_after_first_checkpoint_1mb(self, bft_network):
        await self.state_transfer_after_first_checkpoint(bft_network)

if __name__ == '__main__':
    unittest.main()
