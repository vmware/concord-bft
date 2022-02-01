# Concord
#
# Copyright (c) 2019-2022 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

import os
import random
import unittest


class ApolloTest(unittest.TestCase):

    _SEED = os.getenv('APOLLO_SEED', random.randint(0, 1 << 32))

    @property
    def test_seed(self):
        return self._test_seed

    def setUp(self):
        self._test_seed = random.randint(0, 1 << 32)
        random.seed(self._test_seed)
        print(f'Test seed set to {self._test_seed}')