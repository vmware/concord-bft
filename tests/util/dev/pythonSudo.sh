#!/bin/sh
# This shell script allows running the Python interpreter as root
# NB: the user needs sudo NOPASSWD enabled
sudo PYTHONPATH=$PYTHONPATH python3 "$@"
