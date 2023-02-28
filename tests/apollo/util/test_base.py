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
import traceback
import unittest

import util.eliot_logging as log
from util.eliot_logging import logdir
from functools import wraps
import itertools
import atexit
import sys
from pathlib import Path


def report_cases_failures():
    if ApolloTest.FAILED_CASES:
        Path(logdir()).absolute().mkdir(parents=True, exist_ok=True)
        with open(os.path.abspath(f'{logdir()}/failed_cases.txt'), 'a+') as failed_cases_file:
            for test_name, msg in ApolloTest.FAILED_CASES.items():
                print(f'{test_name} - {msg}', file=failed_cases_file)

atexit.register(report_cases_failures)


class ApolloTest(unittest.TestCase):
    FAILED_CASES = dict()
    _SEED = None

    def register_errors(self):
        if hasattr(self._outcome, 'errors'):
            result = self.defaultTestResult()
            self._feedErrorsToResult(result, self._outcome.errors)
        else:
            result = self._outcome.result

        all_errors = result.errors + result.failures
        all_errors = [test_case for test_case in all_errors if isinstance(test_case[0], unittest.TestCase) and
                      test_case[0].id() not in ApolloTest.FAILED_CASES]
        for test_case, traceback_text in all_errors:
            msg = [x for x in traceback_text.split('\n')[1:]
                   if not x.startswith(' ')][0]
            ApolloTest.FAILED_CASES[test_case.id()] = msg

    @property
    def test_seed(self):
        return self._test_seed

    @classmethod
    def setUpClass(cls):
        cls._SEED = os.getenv('APOLLO_SEED', random.randint(0, 1 << 32))
        random.seed(cls._SEED)

    def setUp(self):
        self._test_seed = ApolloTest._SEED


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


def repeat_test(max_repeats: int, break_on_first_failure: bool, break_on_first_success: bool, test_name=None):
    """
    Runs a test  max_repeats times when both break_on_first_failure and break_on_first_success et to False.
    Only one of break_on_first_failure or break_on_first_success can be True (both can be False).

    break_on_first_success is True: run test up to max_repeats times and breaks after the 1st successful test.
    break_on_first_failure is True: run test up to max_repeats times and breaks after the 1st failing test.
    """
    assert not(break_on_first_failure and break_on_first_success), \
        "Cannot break on first failure and on first success at the same time"
    def decorator(async_fn):
        @wraps(async_fn)
        async def wrapper(*args, **kwargs):
            for i in range(1, max_repeats + 1):
                try:
                    if max_repeats > 1:
                        print(f"Running iteration {i}/{max_repeats}, (break_on_first_failure={break_on_first_failure},"
                              f" break_on_first_success={break_on_first_success})")
                    await async_fn(*args, **kwargs)
                    if break_on_first_success:
                        break
                except Exception as e:
                    is_last = (i == max_repeats) or break_on_first_failure
                    log.log_message(message_type='ERROR - Test attempt failed', run=i, max_repeats=max_repeats,
                                    is_last=is_last, test_name=test_name
                                    )
                    if is_last:
                        raise e
                    else:
                        print(traceback.format_exc())

        return wrapper
    return decorator