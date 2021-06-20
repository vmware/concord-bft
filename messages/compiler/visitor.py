# Concord
#
# Copyright (c) 2020 VMware, Inc. All Rights Reserved.
#
# This product is licensed to you under the Apache 2.0 license (the "License").
# You may not use this product except in compliance with the Apache 2.0 License.
#
# This product may include a number of subcomponents with separate copyright
# notices and license terms. Your use of these subcomponents is subject to the
# terms and conditions of the subcomponent's license, as noted in the LICENSE
# file.

from abc import ABCMeta, abstractmethod


class Visitor(metaclass=ABCMeta):
    """
    All code generators must implement this class for a given language.

    The AST is walked, and at each node a specific callback is fired. These callbacks are used to
    by the visitor to generate relevant code for the given messages. In order to allow complete
    flexibility, the code is not meant to be returned for each callback, but instead accumulated
    and returned as a single string when AST walking is complete. Exceptions should be raised
    during visitation if an error occurs.

    Every primitive type has a single callback with a name identical to its type name. For
    example, type 'bool' has a callback named 'bool'.

    Some compound types have two callbacks: one to start the composition, and one to end it. The
    name of these callbacks is the name of the type with the suffixes '_start' and '_end'. For
    example, type 'list' has callbacks named 'list_start` and 'list_end'. As compound types can
    be nested, it is possible for two `list_start` callbacks to fire in a row, and so on.

    Additionally, kvpairs and maps have callbacks indicating when the keys are done being
    generated. This is to allow adding separating syntax, such as commas, and prevents the
    Visitor from having to keep track of when a given type is in the middle of processing and in
    effect makes it "stateless" for compound types. These types' callbacks are suffixed with
    `key_end`, like `kvpair_key_end`.

    Top level types, 'Msg' and 'Field', cannot be nested, although they also register two
    callbacks, since they must be "opened" and "closed". The top-level 'Enum' type does not
    require more than one callback.

    See 'grammar.ebnf' for details about all types.
    """

    #
    # TOP LEVEL TYPES
    #

    @abstractmethod
    def create_enum(self, name, tags):
      """
        A new enum has been defined.
        Enum definitions are not nestable.

        Args:
            name (str): The name of the Enum
            tags (list(str)): The possible values of the enumeration
        """

    @abstractmethod
    def msg_start(self, name, id):
        """
        A new message has been defined.
        Message definitions are not nestable.

        Args:
            name (str): The name of the message
            id (int): The numeric id of the message
        """
        pass

    @abstractmethod
    def msg_end(self):
        """ The message has been fully defined. """
        pass

    @abstractmethod
    def field_start(self, name, type):
        """
        A new field has been added to the current message.
        Fields are not nestable.

        Args:
            name (str): The name of the field
            type (str): The high level (outermost) type of the field
        """
        pass

    @abstractmethod
    def field_end(self):
        """ The field has been fully defined. """
        pass

    #
    # PRIMITIVE TYPES
    #

    @abstractmethod
    def bool(self):
        pass

    @abstractmethod
    def uint8(self):
        pass

    @abstractmethod
    def uint16(self):
        pass

    @abstractmethod
    def uint32(self):
        pass

    @abstractmethod
    def uint64(self):
        pass

    @abstractmethod
    def int8(self):
        pass

    @abstractmethod
    def int16(self):
        pass

    @abstractmethod
    def int32(self):
        pass

    @abstractmethod
    def int64(self):
        pass

    @abstractmethod
    def string(self):
        pass

    @abstractmethod
    def bytes(self):
        pass

    @abstractmethod
    def msgname_ref(self, name):
        """
        A message name used to refer to a previously defined message.

        Message name references are used only in fields and oneofs in the grammar. However, this
        callback only fires when used directly in a field. It is not necessary in oneofs, as the
        names can be passed directly to the 'oneof'callback.

        Args:
            name (str): The name of the message
        """
        pass

    #
    # COMPOUND TYPES
    #

    @abstractmethod
    def kvpair_start(self):
        pass

    @abstractmethod
    def kvpair_key_end(self):
        pass

    @abstractmethod
    def kvpair_end(self):
        pass

    @abstractmethod
    def list_start(self):
        pass

    @abstractmethod
    def list_end(self):
        pass

    @abstractmethod
    def fixedlist_start(self):
        pass

    @abstractmethod
    def fixedlist_type_end(self):
        pass

    @abstractmethod
    def fixedlist_end(self, size):
        pass

    @abstractmethod
    def map_start(self):
        pass

    @abstractmethod
    def map_key_end(self):
        pass

    @abstractmethod
    def map_end(self):
        pass

    @abstractmethod
    def optional_start(self):
        pass

    @abstractmethod
    def optional_end(self):
        pass

    @abstractmethod
    def oneof(self, msgs):
        """
        A new oneof containing 'msgs' has been defined.

        Args:
           msgs (dict(str, int)): A dict mapping the names of messages to their ids.
        """
        pass

    @abstractmethod
    def enum(self, name):
        """
        An enum field used to refer to a previously defined Enum.

        Args:
            name (str): The name of the Enum
        """
        pass
