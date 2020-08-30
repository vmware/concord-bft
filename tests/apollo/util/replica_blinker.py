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


import argparse
import random
import signal
import subprocess
import sys
import time
import basic_logger


BLINKING_INTERVAL_SECONDS = (1, 10)
stopped = False


def signal_handler(signal, frame):
    global stopped
    stopped = True
    logger.info("Received signal {}, stopping replica blinker".format(signal))


def main():
    parser = argparse.ArgumentParser(
            description='Starts and stops the given replica with random interval')
    parser.add_argument('-cmd',
                        help='Command line to start replica')
    start_replica_cmd = parser.parse_args().cmd.split(',')
    logger.info("Command line to start replica: {}".format(start_replica_cmd))

    try:
        blinking_replica = None
        global stopped
        while not stopped:
            blinking_replica = subprocess.Popen(start_replica_cmd, close_fds=True)
            logger.info("Started replica")
            time.sleep(random.uniform(*BLINKING_INTERVAL_SECONDS)/10)

            blinking_replica.kill()
            blinking_replica.wait()
            logger.info("Stopped replica")
            blinking_replica = None
            time.sleep(random.uniform(*BLINKING_INTERVAL_SECONDS)/10)
    except Exception as e:
        logger.exception("Error occured while blinking replica: {}".format(e))
        if blinking_replica:
            blinking_replica.kill()
            blinking_replica.wait()
        sys.exit(1)
    logger.info("Replica blinker is stopped")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    logger = basic_logger.get_logger(__name__)
    main()
