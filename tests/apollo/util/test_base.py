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

def repeat_test(max_repeats: int, break_on_first_failure: bool, break_on_first_success: bool):
    """
    Runs a test  max_repeats times when both break_on_first_failure and break_on_first_success et to False.
    Only one of break_on_first_failure or break_on_first_success can be True (both can be False).

    break_on_first_success is True: run test up to max_repeats times and breaks after the 1st successful test.
    break_on_first_failure is True: run test up to max_repeats times and breaks after the 1st failing test.
    """
    assert not (break_on_first_failure and break_on_first_success), \
        "both flags break_on_first_failure and break_on_first_success cannot be enabled at the same time!"
    def decorator(async_fn):
        @wraps(async_fn)
        async def wrapper(*args, **kwargs):
            for i in range(1, max_repeats + 1):
                try:
                    if (max_repeats > 1):
                        print(f"Running iteration {i}/{max_repeats}, (break_on_first_failure={break_on_first_failure},"
                              f" break_on_first_success={break_on_first_success})")
                    await async_fn(*args, **kwargs)
                    if break_on_first_success:
                        break
                except Exception as e:
                    log.log_message(message_type='Test attempt failed', run=i, max_repeats=max_repeats,
                                    break_on_first_failure=break_on_first_failure)
                    if i == max_repeats or break_on_first_failure:
                        raise e
        return wrapper
    return decorator