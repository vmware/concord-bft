from eliot import start_action, start_task, to_file, add_destinations, log_call, log_message
from datetime import datetime
import os
import sys


def set_file_destination():
    test_name = os.environ.get('TEST_NAME')

    if not test_name:
        now = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
        test_name = f"apollo_run_{now}"

    logs_dir = '../../build/tests/apollo/logs/'
    test_dir = f'{logs_dir}{test_name}'
    test_log = f'{test_dir}/{test_name}.log'

    if not os.path.isdir(logs_dir):
        # Create logs directory if not exist
        os.mkdir(logs_dir)

    if not os.path.isdir(test_dir):
        # Create directory for the test logs
        os.mkdir(test_dir)

    if os.path.isfile(test_log):
        # Clean logs if file already exist
        open(test_log, "w").close()

    # Set the log file path
    to_file(open(test_log, "a"))


# Set logs to the console
def stdout(message):
    if message is not "":
        print(message)


if os.environ.get('KEEP_APOLLO_LOGS', "").lower() in ["true", "on"]:
    # Uncomment to see logs in console
    #add_destinations(stdout)
    set_file_destination()
