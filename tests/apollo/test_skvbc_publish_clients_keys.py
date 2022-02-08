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

from util.test_base import ApolloTest
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
            "-w", str(True)
            ]


class SkvbcPublishClientsKeysTest(ApolloTest):

    __test__ = False  # so that PyTest ignores this test scenario

    @with_trio
    @with_bft_network(start_replica_cmd)
    async def test_publish_client_keys(self, bft_network):
        """
        Test that client keys are published
        """
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        bft_network.start_all_replicas()

        with trio.fail_after(seconds=5):
            for replica_id in bft_network.all_replicas():
                while True:
                    with trio.move_on_after(seconds=1):
                        try:
                            clients_keys_published_status = await bft_network.get_metric(replica_id, bft_network, 'Statuses', "clients_keys_published","KeyExchangeManager")
                            if  clients_keys_published_status == 'False' :
                                await trio.sleep(0.1)
                                continue
                        except trio.TooSlowError:
                            print(f"Replica {replica_id} was not able to publish client keys on start")
                            raise KeyExchangeError
                        else:
                            assert clients_keys_published_status == 'True'
                            break


if __name__ == '__main__':
    unittest.main()
