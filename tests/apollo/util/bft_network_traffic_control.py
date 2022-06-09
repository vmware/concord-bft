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
import os

class NetworkTrafficControl():
    """Conatins the functions capable of controlling traffic in network"""

    def __init__(self):
        pass
    
    def __enter__(self):
        """context manager method for 'with' statements"""
        return self

    def __exit__(self, *args):
        self.del_loop_back_interface_delay()
    
    def del_loop_back_interface_delay(self):
        assert 0 == os.system('tc qdisc del dev lo root') == 0,\
                 "tc delte command failed, need to be removed orelse it will affect the replica"
    
    def put_loop_back_interface_delay(self, delay_factor):
        assert 0 == os.system('tc qdisc add dev lo root netem delay '+\
                         str(delay_factor) + 'ms'),\
                         "Make sure tc is installed in the replica"

