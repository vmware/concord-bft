#!/usr/bin/env python3

import argparse
import tatsu

from pprint import pprint
from exceptions import CmfParseError
from semantics import CmfSemantics, SymbolTable


def parse(grammar, cmf):
    """
    Take a tatsu styled EBNF grammar specifying the concord metadata format (CMF) and generate a parser from it. Then parse a cmf file with the generated parser. Apply an initial analysis/typechecking pass via CMFSemantics and transform the parse tree to an AST. When parsing, save all the seen message names and ids in a symbol table. Finally return the AST and symbol table to the caller.
    """
    parser = tatsu.compile(grammar)
    symbol_table = SymbolTable()
    ast = parser.parse(cmf,
                       semantics=CmfSemantics(symbol_table),
                       parseinfo=True)
    return ast, symbol_table


def translate(ast, language, namespace):
    if language == "cpp":
        print("Generating C++ source code")
        from cpp import cppgen

        return cppgen.translate(ast, namespace)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compile a concord message format file")
    parser.add_argument("--input",
                        help="The concord message format (CMF) input filename",
                        required=True)
    parser.add_argument("--output", help="The output filename", required=True)
    parser.add_argument("--language",
                        help="The output language",
                        choices=["cpp"],
                        required=True)
    parser.add_argument(
        "--namespace",
        help="Add a namespace if required by the given language")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    with open("./grammar.ebnf") as f:
        grammar = f.read()
        with open(args.input) as f2:
            cmf = f2.read()
        ast, symbol_table = parse(grammar, cmf)
        # Uncomment to show the generated AST for debugging purposes
        # pprint(ast)
        code = translate(ast, args.language, args.namespace)
        with open(args.output, "w") as f3:
            f3.write(code)
