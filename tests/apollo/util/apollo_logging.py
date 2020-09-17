from eliot import start_action, start_task, to_file, add_destinations
from datetime import datetime
import os


def set_file_destination():
    test_name = os.environ.get('TEST_NAME')

    # Create logs directory if not exist
    if not os.path.isdir("logs"):
        os.mkdir("logs")

    if test_name is None:
        # TESTS_NAME isn't part of the environment variable
        time = datetime.now().strftime("%d-%b-%Y-%H:%M:%S")
        test_name = f"logs/{time}.log"
    else:
        print(test_name)
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
