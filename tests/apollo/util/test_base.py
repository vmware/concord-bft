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
import util.eliot_logging as log
from functools import wraps
import itertools


class ApolloTest(unittest.TestCase):

    @property
    def test_seed(self):
        return self._test_seed

    def setUp(self):
        self._test_seed = os.getenv('APOLLO_SEED', random.randint(0, 1 << 32))
        random.seed(self._test_seed)
        print(f'Test seed set to {self._test_seed}')


def parameterize(**parameterize_kwargs):
    """
    """
    def decorator(async_fn):
        @wraps(async_fn)
        async def wrapper(*args, **kwargs):
            test_instance = args[0]
            new_dict = dict()
            for key in parameterize_kwargs:
                assert key not in kwargs, f"Parameter {key} already passed to {test_instance}"
                new_dict[key] = [(key, value) for value in parameterize_kwargs[key]]

            for kv_tuples in itertools.product(*new_dict.values()):
                with test_instance.subTest(params=kv_tuples):
                    for kv_tuple in kv_tuples:
                        kwargs[kv_tuple[0]] = kv_tuple[1]
                    await async_fn(*args, **kwargs)
        return wrapper

    return decorator

def retry_test(max_retries: int):
    """
    Runs a test multiple times until a run succeeds
    """

    def decorator(async_fn):
        @wraps(async_fn)
        async def wrapper(*args, **kwargs):
            for i in range(1, max_retries + 1):
                try:
                    await async_fn(*args, **kwargs)
                    break
                except Exception as e:
                    log.log_message(message_type='Test attempt failed', run=i, max_retries=max_retries)
                    if i == max_retries:
                        raise e

        return wrapper
    return decorator