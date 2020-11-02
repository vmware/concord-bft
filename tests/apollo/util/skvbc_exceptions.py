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

class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class BadReplyError(Exception):
    """An invalid reply was returned from the client"""
    pass

class ReadTimeoutError(Exception):
    """A read was expected to succeed but timed out instead"""
    pass

##
## Exceptions for skvbc_linearizability
##
class ConflictingBlockWriteError(Error):
    """The same block was already written by a different conditional write"""
    def __init__(self, block_id, block, new_request):
        self.block_id = block_id
        self.block = block
        self.new_request = new_request

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  block_id={self.block_id}\n'
           f'  block={self.block}\n'
           f'  new_request={self.new_request}\n')

class StaleReadError(Error):
    """
    A conditional write did not see that a key in its readset was written after
    the block it was attempting to read from, but before the block the write
    created. As an example, The readset block version was X, an update was made
    to a key in the readset in block X+1, and this write successfully wrote
    block X+2.

    In our example the parameters to the constructor would be set as:

        readset_block_id = X
        block_with_conflicting_writeset = X + 1
        block_being_checked = X + 2

    """
    def __init__(self,
                 readset_block_id,
                 block_with_conflicting_writeset,
                 block_being_checked):
        self.readset_block_id = readset_block_id
        self.block_with_conflicting_writeset = block_with_conflicting_writeset
        self.block_being_checked = block_being_checked

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  readset_block_id={self.readset_block_id}\n'
           f'  block_with_conflicting_writeset='
           f'{self.block_with_conflicting_writeset}\n'
           f'  block_being_checked={self.block_being_checked}\n')


class NoConflictError(Error):
    """
    A conditional write failed when it should have succeeded.

    There were no concurrent write requests that actually conflicted with the
    stale request.
    """
    def __init__(self, failed_req, causal_state):
        self.failed_req = failed_req
        self.causal_state = causal_state

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  failed_req={self.failed_req}\n'
           f'  causal_state={self.causal_state}\n')

class InvalidReadError(Error):
    """
    The values returned by a read did not linearize given the state of the
    blockchain and concurrent requests.
    """
    def __init__(self, read, concurrent_requests):
        self.read = read
        self.concurrent_requests = concurrent_requests

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  read={self.read}\n'
           f'  concurrent_requests={self.concurrent_requests}\n')

class PhantomBlockError(Error):
    """
    A block was created with a given set of kvpairs that no write request
    generated.
    """
    def __init__(self, block_id, kvpairs, matched_blocks, unmatched_requests):
        self.block_id = block_id
        self.kvpairs = kvpairs
        self.matched_blocks = matched_blocks
        self.unmatched_requests = unmatched_requests

    def __repr__(self):
        return (f'{self.__class__.__name__}:\n'
           f'  block_id={self.block_id}\n'
           f'  kvpairs={self.kvpairs}\n'
           f'  matched_blocks={self.matched_blocks}\n'
           f'  unmatched_requests={self.unmatched_requests}\n')
