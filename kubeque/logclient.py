import time
from termcolor import colored, cprint
from google.cloud import logging
import datetime

def print_error_lines(lines):
    for line in lines:
        print(colored(line, "red"))

def print_entry(entry):
    payload = entry.payload
    if payload[-1] == "\n":
        payload = payload[:-1]
    timestamp = entry.timestamp.astimezone()
    payload_lines = payload.split("\n")
    if payload_lines[-1] == "":
        del payload_lines[-1]
    prefix = None
    for line in payload_lines:
        if prefix is None:
            prefix = "[{}]".format(timestamp.strftime("%H:%M:%S"))
            print(colored(prefix, "green"), colored(line, "yellow"))
        else:
            print(colored(" "*len(prefix), "white"), colored(line, "yellow"))

def _get_log_stream(client, project_id, task_id, next_token_ref, time_between_polls=2):
    timestamp_str = ( datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=0))) - datetime.timedelta(minutes=10) ).isoformat('T')
    last_batch_size = 0
    start_index = 0
    # this feels very complicated, but seems to work with the API that I've been given. The issue is I've only got the previous page,
    # so when I fetch the next page a second time, keep track of how many records into it to skip.
    # perhaps changing the iterator into an explict fetch by page token might make the logic clearer
    while True:
        iterator = client.list_entries(filter_="logName=\"projects/{}/logs/{}\" AND Timestamp > \"{}\"".format(project_id, task_id, timestamp_str), page_token=next_token_ref[0], page_size=50)
        for page in iterator.pages:
            entries = list(page)
            if iterator.next_page_token is not None:
                next_token_ref[0] = iterator.next_page_token
            else:
                last_batch_size = len(entries)

            for entry in entries[start_index:]:
                print_entry(entry)
            start_index = 0

        if last_batch_size < 50:
            start_index = last_batch_size

        last_poll_complete = time.time()
        yield
        # make sure we can't hit the logging API too frequently
        time_remaining = time_between_polls - (time.time() - last_poll_complete)
        if time_remaining > 0:
            time.sleep(time_remaining)

class LogMonitor:
    def __init__(self, project_id, task_id):
        self.client = logging.Client(project=project_id)
        self.project_id = project_id
        self.task_id = task_id
        self.next_token_ref = [None]
        self.reset()
    
    def reset(self):
        self.stream = _get_log_stream(self.client, self.project_id, self.task_id, self.next_token_ref)
    
    def poll(self):
        next(self.stream)    


