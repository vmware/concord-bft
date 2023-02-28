from functools import lru_cache
from eliot import start_action, start_task, to_file, add_destinations, log_call, log_message, remove_destination
from datetime import datetime
import os
import sys
import traceback
import atexit
from pathlib import Path
from eliot import register_exception_extractor
register_exception_extractor(Exception, lambda e: {"traceback": traceback.format_exc()})

@lru_cache(maxsize=None)
def logdir_timestamp():
    timestamp_file = Path(f'../../build/timestamp')
    if not timestamp_file.exists():
        print(f"Timestamp file {timestamp_file} doesn't exist", file=sys.stderr)
        return datetime.now().strftime("%y-%m-%d_%H-%M-%S")
    test_dir_name = timestamp_file.read_text().split('\n')[0]
    assert len(test_dir_name) > 0
    return test_dir_name


@lru_cache(maxsize=None)
def logdir():
    relative_apollo_logs = 'tests/apollo/logs'
    relative_current_run_logs = f'{relative_apollo_logs}/{logdir_timestamp()}'
    return f'../../build/{relative_current_run_logs}'


def set_file_destination():
    test_name = os.environ.get('TEST_NAME')

    if not test_name:
        now = logdir_timestamp()
        test_name = f"apollo_run_{now}"

    if os.environ.get('BLOCKCHAIN_VERSION', default="1").lower() == "4":
        test_name = test_name + "_v4"

    relative_apollo_logs = 'tests/apollo/logs'
    relative_current_run_logs = f'{relative_apollo_logs}/{logdir_timestamp()}'
    logs_dir = f'../../build/{relative_current_run_logs}'
    test_dir = f'{logs_dir}/{test_name}'
    test_log = f'{test_dir}/{test_name}.log'

    logs_shortcut = Path('../../build/apollogs')
    logs_shortcut.mkdir(exist_ok=True)
    all_logs = logs_shortcut / 'all'
    if not all_logs.exists():
        all_logs.symlink_to(target=Path(f'../{relative_apollo_logs}'), target_is_directory=True)

    Path(test_dir).mkdir(parents=True, exist_ok=True)
    latest_shortcut = Path(logs_shortcut) / 'latest'

    if latest_shortcut.exists():
        latest_shortcut.unlink()
    latest_shortcut.symlink_to(target=Path(f'../{relative_current_run_logs}'), target_is_directory=True)


    test_log_file = open(test_log, "a+")
    to_file(test_log_file)
    atexit.register(lambda: test_log_file.close())


def format_eliot_message(message):
    if message.get("action_status", "") == "succeeded":
        return

    additional_fields = [(key, val) for key, val in message.items() if key not in
                         ("action_type", "action_status", "result", "task_uuid",
                          "timestamp", "task_level", "message_type")]

    fields = [datetime.fromtimestamp(message["timestamp"]).strftime("%d/%m/%Y %H:%M:%S.%f"),
              message.get("message_type", None),
              message.get("action_type", None),
              message.get("action_status", None),
              message.get("result", None),
              str(additional_fields).replace('\\n', '\n'),
              message["task_uuid"],
              ]

    return f' - '.join([field for field in fields if field])


# Set logs to the console
def eliot_stdout(message):
    formatted = format_eliot_message(message)
    if formatted:
        print(formatted)


if os.getenv('APOLLO_LOG_STDOUT', False):
    add_destinations(eliot_stdout)

if os.environ.get('KEEP_APOLLO_LOGS', "").lower() in ["true", "on"]:
    # Uncomment to see logs in console
    set_file_destination()

