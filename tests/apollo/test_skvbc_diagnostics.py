# Concord
#
# Copyright (c) 2022 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

import os.path
import sys
from pathlib import Path

sys.path.append(os.path.abspath(Path(__file__).parent.parent.parent / "util" / "pyclient"))
from bft_config import BFTConfig
from util.test_base import ApolloTest, parameterize
from util.bft import with_trio, with_bft_network, KEY_FILE_PREFIX
from util import skvbc as kvbc
from typing import TYPE_CHECKING, Sequence

sys.path.append(str(Path(__file__).absolute().parent.parent.parent / "diagnostics"))
import concord_ctl_logic as concord_ctl

if TYPE_CHECKING:
    from util.bft import BftTestNetwork


def replica_diagnostic_server_port(replica_id):
    """
    Returns a unique port for each replica
    All the replicas run in the same container and therefore share an IP address
    """
    return 27500 + replica_id


def start_replica_cmd(builddir, replica_id):
    """
    Return a command that starts an skvbc replica when passed to
    subprocess.Popen.

    Note each arguments is an element in a list.
    """
    statusTimerMilli = "500"
    view_change_timeout_milli = "10000"
    path = os.path.join(builddir, "tests", "simpleKVBC", "TesterReplica", "skvbc_replica")
    return [path,
            "-k", KEY_FILE_PREFIX,
            "-i", str(replica_id),
            "-s", statusTimerMilli,
            "-v", view_change_timeout_milli,
            "-e", str(True),
            "--diagnostics-port", f"{replica_diagnostic_server_port(replica_id)}"
            ]


class SkvbcDiagnoticsTest(ApolloTest):
    """
    This class tests the diagnostic server in skvbc_replica
    It uses the concord-ctl utility's functionality to communicate with the server
    """
    __test__ = False  # so that PyTest ignores this test scenario
    # There is no need to rotate keys as it slows the
    ROTATE_KEYS = False

    @property
    def hostname(self):
        """
        Hostname for diagnostics server
        """
        return 'localhost'

    def run_ctl_command(self, command_args: Sequence[str], replica_id:int = 0):
        return concord_ctl.run(self.hostname, replica_diagnostic_server_port(replica_id),
                               command_args, print_output=False)

    def validate_status_get_command(self, command_args: Sequence[str], replica_id:int = 0):
        invalid_response = '*--STATUS_NOT_FOUND--*'
        output = self.run_ctl_command(['status', 'get'] + list(command_args), replica_id)
        assert invalid_response not in output
        return output

    def validate_status_describe_command(self, command_args: Sequence[str], replica_id:int = 0):
        invalid_response = '*--DESCRIPTION_NOT_FOUND--*'
        output = self.run_ctl_command(['status', 'describe'] + list(command_args), replica_id)
        assert invalid_response not in output
        return output

    @with_trio
    @parameterize(is_status_valid=[True, False])
    @with_bft_network(start_replica_cmd, bft_configs=[BFTConfig(n=4)], rotate_keys=ROTATE_KEYS)
    async def test_statuses(self, bft_network: 'BftTestNetwork', is_status_valid: bool):
        """
        This test checks that statuses are returned when the request is valid and that correct
        error values are returned when statuses are invalid
        """
        bft_network.start_all_replicas()
        skvbc = kvbc.SimpleKVBCProtocol(bft_network)
        # Make sure that the system is online
        await skvbc.send_n_kvs_sequentially(1)

        validation_functions = [self.validate_status_get_command, self.validate_status_describe_command]
        if is_status_valid:
            statuses = self.run_ctl_command(['status', 'list']).strip().split('\n')
            assert len(statuses), "There must be at least one registered status"

            # Include multi parameter commands validation
            status_combinations = [[status] for status in statuses] + [statuses]
            for status in status_combinations:
                for function in validation_functions:
                    function(status)
            return

        invalid_status = 'INVALID'
        for function in validation_functions:
            with self.assertRaises(AssertionError):
                function([invalid_status])
