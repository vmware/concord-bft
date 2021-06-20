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

# Add the pyclient directory to $PYTHONPATH


import os
import subprocess
from util import eliot_logging as log


class BlinkingReplica(object):
    """docstring for BlinkingReplica"""

    def __init__(self):
        super(BlinkingReplica, self).__init__()
        self.blinker_process = None

    def __enter__(self):
        """context manager method for 'with' statements"""
        return self

    def __exit__(self, *args):
        """context manager method for 'with' statements"""
        self.stop_blinking()

    def start_blinking(self,start_replica_cmd):
        self.start_replica_cmd = start_replica_cmd
        self.blinker_process = subprocess.Popen(self._cmd_line(), close_fds=True)
        log.log_message(message_type=f'Started blinking: {self._cmd_line()}')

    def stop_blinking(self):
        if self.blinker_process:
            self.blinker_process.terminate()
            if self.blinker_process.wait() != 0:
                raise Exception("Error occured while while stopping the blinker process")
        log.log_message(message_type="Stopped blinking")

    def _cmd_line(self):
        return ['python3',
                os.path.join(os.path.dirname(os.path.abspath(__file__)), 'replica_blinker.py'),
                '-cmd',
                ",".join(self.start_replica_cmd)]
