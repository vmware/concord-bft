from eliot import start_action, start_task, to_file, add_destinations, log_call
from datetime import datetime
import os
import sys


def set_file_destination():
    storage_type = os.environ.get('STORAGE_TYPE')
    tests_names = [m for m in sys.modules.keys() if "test_" in m]

    if len(tests_names) > 1:
        # Multiple Apollo tests modules loaded, test name unknown.
        now = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
        test_name = f"apollo_run_{now}"
    else:
        # Single Apollo module loaded, test name known.
        test_name = f"{tests_names.pop()}_{storage_type}"

    # Create logs directory if not exist
    if not os.path.isdir("logs"):
        os.mkdir("logs")

    test_name = f"logs/{test_name}.log"

    if os.path.isfile(test_name):
        # Clean logs if file already exist
        open(test_name, "w").close()

    # Set the log file path
    to_file(open(test_name, "a"))


# Set logs to the console
def stdout(message):
    if message is not "":
        print(message)


add_destinations(stdout)
set_file_destination()
