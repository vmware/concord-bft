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

class BftMetrics:
    """ A wrapper class that helps to access individual metrics """ 

    def __init__(self, clients):
        # clients is a dictionary of MetricsClient by replica_id 
        self.clients = clients

    def __enter__(self):
        """context manager method for 'with' statements"""
        return self

    def __exit__(self, *args):
        """context manager method for 'with' statements"""
        for client in self.clients.values():
            client.__exit__()

    async def get(self, replica_id, component_name, type_, key):
        """ 
        Return the value of a key of given type for the given component at
        the given replica. 
        """
        metrics = await self.clients[replica_id].get()
        for component in metrics['Components']:
            if component['Name'] == component_name:
                return component[type_][key]
        raise KeyError
