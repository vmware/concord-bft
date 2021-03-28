from exceptions import CmfParseError


class MsgWithOneof:
    def __init__(self, name):
        self.name = name
        self.message_names_in_oneof = set()


class SymbolTable:
    def __init__(self):
        # Used during semantic analysis to detect duplicates
        # Line numbers are stored as values
        self.msg_ids = dict()
        self.msg_names = dict()
        self.enum_names = dict()


class CmfSemantics(object):
    """
    Perform basic type checking and conversion on the AST

    Function names are the names of rules in grammar.ebnf
    See https://tatsu.readthedocs.io/en/4.4/semantics.html
    """

    def __init__(self, symbol_table):
        self.symbol_table = symbol_table

    def msgid(self, ast):
        """ Check that each message id is unique and fits in a 32 bit integer """
        id = int(ast.id)
        if id < 0 or id > pow(2, 32):
            raise CmfParseError(ast.parseinfo,
                                'Message ID: "{}" must fit in a uint32'.format(id))
        if id in self.symbol_table.msg_ids:
            raise CmfParseError(ast.parseinfo, 'Message ID: "{}" already defined on line {}'.format(
                id, self.symbol_table.msg_ids[id]))
        # parseinfo.line is zero-based
        self.symbol_table.msg_ids[id] = ast.parseinfo.line + 1
        return id

    def toplevel_ref(self, ast):
        """ Determine if the name in the ast is a Msg or Enum and tag it. """
        if ast.name in self.symbol_table.msg_names.keys():
            return {'msg': ast.name}
        if ast.name in self.symbol_table.enum_names.keys():
            return {'enum': ast.name}
        raise CmfParseError(
            ast.parseinfo, "Messages or Enums must be defined before they are referenced: {}".format(ast.name))

    def msgname(self, ast):
        """ Check that message names are unique """
        if ast.name in self.symbol_table.msg_names:
            raise CmfParseError(ast.parseinfo, 'Message: "{}" already defined on line {}'.format(
                ast.name, self.symbol_table.msg_names[ast.name]))
        if ast.name in self.symbol_table.enum_names:
            raise CmfParseError(ast.parseinfo, 'Message: "{}" cannot have the same name as the Enum defined on line {}'.format(
                ast.name, self.symbol_table.enum_names[ast.name]))
        # parseinfo.line is zero-based
        self.symbol_table.msg_names[ast.name] = ast.parseinfo.line + 1
        return ast.name

    def enumname(self, ast):
        """ Check that enum names are unique """
        if ast.name in self.symbol_table.enum_names:
            raise CmfParseError(ast.parseinfo, 'Enum: "{}" already defined on line {}'.format(
                ast.name, self.symbol_table.enum_names[ast.name]))
        if ast.name in self.symbol_table.msg_names:
            raise CmfParseError(ast.parseinfo, 'Enum: "{}" cannot have the same name as the Msg defined on line {}'.format(
                ast.name, self.symbol_table.msg_names[ast.name]))
        # parseinfo.line is zero-based
        self.symbol_table.enum_names[ast.name] = ast.parseinfo.line + 1
        return ast.name

    def msg(self, ast):
        """ Check that each field in a message has a unique name """
        field_names = set()
        for field in ast.fields:
            if field.name in field_names:
                raise CmfParseError(
                    ast.parseinfo, 'Message: "{}" contains duplicate field: "{}"'.format(ast.name, field.name))
            field_names.add(field.name)
        return ast

    def enum_def(self, ast):
        """ Ensure that an enum fits in a uint8 and that it has no duplicate fields """
        if (len(ast.tags) > 256):
            raise CmfParseError(ast.parseinfo, 'Enum: "{}" contains more than 256 entries. It must fit in a uint8_t'.format(ast.name))
        tags = set()
        for tag in ast.tags:
            if tag in tags:
                raise CmfParseError(
                    ast.parseinfo, 'Enum: "{}" contains duplicate tag: "{}"'.format(ast.name, tag))
            tags.add(tag)
        return ast
