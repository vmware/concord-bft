#!/usr/bin/env python3

import argparse
import socket
import sys
from contextlib import closing

def main(params):
    parser = args_parser()
    args, remaining = parser.parse_known_args(params)
    run(args.host, args.port, remaining)


def run(host, port, remaining_args, print_output=True):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.connect((host, port))
        cmd = (' '.join(remaining_args) + '\n').encode('utf-8')
        s.send(cmd)
        data = s.recv(64 * 1024)
        result = data.decode()
        if print_output:
            print(result)
        s.close()
        return result


def args_parser():
    parser = argparse.ArgumentParser(description="Concord Diagnostics CLI")
    parser.add_argument(
        '--host',
        help='Host that the diagnostics server is listening on',
        default="127.0.0.1")
    parser.add_argument(
        '--port',
        help='Port that the diagnostics server is listening on',
        type=int,
        default=6888)
    return parser


if __name__ == "__main__":
    main(sys.argv[1:])
