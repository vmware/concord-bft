#!/usr/bin/python3

import unittest
import sys
import os
from pathlib import Path

"""
This script gets a target folder and a format character (h,f,s,c,t), discovers and prints all Apollo unittest unit test 
under the given folder.
"""

last_suite=None
last_class=None
count=0
usage_str=f"usage: {__file__} target_dir (h)ierarchy | (f)lat | (s)uites | (c)lasses | (t)ests"
printed = []

def print_suite(suite, mode):
    global last_suite, last_class, count, printed
    if hasattr(suite, '__iter__'):
        for x in suite:
            print_suite(x, mode)
    else:
        s1 = str(suite).replace("(", "").replace(")", "").split(" ")
        s2 = s1[1].split(".")
        s2.append(s1[0])

        if mode == "h":
            # Hierarchy doesn't track if already printed. it prints everything
            if last_suite != s2[0]:
                print(s2[0], end =".")
                last_suite = s2[0]
            else:
                print(" " * (len(s2[0])+1), end="")
            if last_class != s2[1]:
                print(s2[1], end =".")
                last_class = s2[1]
            else:
                print(" " * (len(s2[1])+1), end="")
            print(f"{s2[2]}")
            count += 1
        elif mode == "f":
            full_test = f"{s2[0]}.{s2[1]}.{s2[2]}"
            if full_test not in printed:
                print(full_test)
                printed.append(full_test)
                count += 1
        elif mode == "t":
            test = f"{s2[2]}"
            if test not in printed:
                print(f"{s2[2]}")
                printed.append(test)
                count += 1
        elif mode == 's':
            suite = s2[0]
            if (last_suite != suite) and (suite not in printed):
                print(suite)
                last_suite = suite
                count += 1
                printed.append(suite)
        elif mode == 'c':
            _class = f"{s2[0]}.{s2[1]}"
            if (last_class != _class) and (_class not in printed):
                print(_class)
                last_class = _class
                count += 1
                printed.append(_class)
        else:
            print(usage_str)
            sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(usage_str)
        sys.exit(1)
    target_dir = sys.argv[1]
    mode = sys.argv[2]
    target_dir = Path(target_dir).resolve()
    if not os.path.isdir(target_dir):
        print(f'{target_dir} is not a directory!')
        sys.exit(1)
    os.chdir(target_dir)
    print_suite(unittest.defaultTestLoader.discover('.'), mode)
    print("=" * 16)
    print("Total entries: " + str(count))
