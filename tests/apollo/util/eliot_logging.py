from eliot import start_action, start_task, to_file, add_destinations, log_call
from datetime import datetime
import os
import sys


def set_file_destination():
    storage_type = os.environ.get('STORAGE_TYPE')
    tests_names = [m for m in sys.modules.keys() if m.startswith("test_")]
    if len(tests_names) > 1:
        # Multiple Apollo tests modules loaded, test name unknown.
        now = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
        test_name = f"apollo_run_{now}"
    else:
        # Single Apollo module loaded, test name known.
        test_name = f"{tests_names.pop()}_{storage_type}"

    logs_dir = '/tmp/apollo/'
    test_dir = f'{logs_dir}{test_name}'
    test_log = f'{test_dir}/apollo_{test_name}.log'

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


add_destinations(stdout)
set_file_destination()
