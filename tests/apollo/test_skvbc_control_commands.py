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
import os.path
import random
import unittest
from os import environ

import trio

from util import blinking_replica
from util import skvbc as kvbc
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    viewChangeTimeoutMilli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", viewChangeTimeoutMilli,
            "-p" if os.environ.get('BUILD_ROCKSDB_STORAGE', "").lower()
                    in set(["true", "on"])
                 else "",
            "-t", os.environ.get('STORAGE_TYPE')]


class SkvbcControlCommandsTest(unittest.TestCase):

    # __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd,  selected_configs=lambda n, f, c: n == 4 and c == 0)
    async def test_wedge_command(self, bft_network):
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        client = bft_network.random_client()
        rep = await client.write(bytearray(), special_flags=0x9)
        await skvbc.write_known_kv()
        for replica_id in range(bft_network.config.n):
            with trio.fail_after(seconds=60):
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            key = ['replica', 'Gauges', 'OnCallBackOfSuperStableCP']
                            value = await bft_network.metrics.get(replica_id, *key)
                            if value == 0:
                                continue
                        except KeyError:
                            # metrics not yet available, continue looping
                            print(f"KeyError! OnCallBackOfSuperStableCP not yet available.")
                        else:
                            self.assertEqual(value, 1)
                            break
        try:
            await skvbc.write_known_kv()
        except trio.TooSlowError:
            return
        else:
            self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
