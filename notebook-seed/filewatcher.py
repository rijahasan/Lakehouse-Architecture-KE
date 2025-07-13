import os
import time
import logging
import threading
import subprocess
import queue
import sys
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler

WATCH_DIRECTORY = "/mnt/HabibData"
RAW_SCRIPT = "/incremental_load"
SIGNAL_DIRECTORY = "/mnt/inc_processed"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

file_queue = queue.Queue()


def stream_output(pipe, log_fn):
    for line in iter(pipe.readline, ''):
        log_fn(line.strip())
    pipe.close()


def is_file_stable(file_path, stable_duration=35):
    try:
        last_modified_time = os.path.getmtime(file_path)
    except FileNotFoundError:
        logging.warning(f"[WARNING] File not found: {file_path}")
        return False

    start_time = time.time()

    while time.time() - start_time < stable_duration:
        time.sleep(1)
        try:
            current_modified_time = os.path.getmtime(file_path)
        except FileNotFoundError:
            logging.warning(f"[WARNING] File was removed during stability check: {file_path}")
            return False

        if current_modified_time != last_modified_time:
            logging.info(f"[INFO] File {file_path} modified during stability wait. Restarting timer.")
            last_modified_time = current_modified_time
            start_time = time.time()

    logging.info(f"[INFO] File {file_path} is stable for {stable_duration} seconds.")
    return True


class DirectoryWatcher(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        if event.src_path.lower().endswith('.csv') and 'Ageing' in event.src_path:
            logging.info(f"[INFO] New CSV file detected: {event.src_path}")
            if is_file_stable(event.src_path):
                file_queue.put(event.src_path)
                logging.info(f"[INFO] File queued for processing: {event.src_path}")
            else:
                logging.info(f"[INFO] File {event.src_path} is still being updated. Skipping trigger.")
        else:
            logging.info(f"[INFO] Ignored file: {event.src_path}")


def process_files():
    while True:
        file_path = file_queue.get()
        if file_path is None:
            break

        python_exec = sys.executable
        script_command = [python_exec, RAW_SCRIPT, file_path]
        logging.info(f"Executing: {' '.join(script_command)}")

        try:
            process = subprocess.Popen(
                script_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            threading.Thread(target=stream_output, args=(process.stdout, logging.info)).start()
            threading.Thread(target=stream_output, args=(process.stderr, logging.error)).start()

            return_code = process.wait()
            if return_code != 0:
                logging.error(f"Script exited with code {return_code}")
            else:
                wait_for_completion()

        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
        finally:
            file_queue.task_done()


def wait_for_completion():
    signal_file = os.path.join(SIGNAL_DIRECTORY, "done.signal")
    logging.info("[INFO] Waiting for incremental_load completion signal...")

    timeout = 600
    start_time = time.time()

    while time.time() - start_time < timeout:
        if os.path.exists(signal_file):
            logging.info("[INFO] Incremental load completed! Acknowledging filewatcher.")
            os.remove(signal_file)
            return
        time.sleep(1)

    logging.warning("[WARNING] Incremental load script did not send completion signal in time.")


def start_watching():
    logging.info("[INFO] Starting directory watcher...")
    event_handler = DirectoryWatcher()
    observer = PollingObserver()
    observer.schedule(event_handler, WATCH_DIRECTORY, recursive=False)
    observer.start()

    processor_thread = threading.Thread(target=process_files)
    processor_thread.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("[INFO] Stopping directory watcher...")
        observer.stop()
        file_queue.put(None)  # Signal to stop the processor thread

    observer.join()
    processor_thread.join()


if __name__ == "__main__":
    logging.info("[INFO] Filewatcher script is running...")
    start_watching()
