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

class AlreadyRunningError(Error):
    def __init__(self, replica):
        self.replica = replica

class AlreadyStoppedError(Error):
    def __init__(self, replica):
        self.replica = replica

class BadReplyError(Error):
    def __init__(self):
        pass
