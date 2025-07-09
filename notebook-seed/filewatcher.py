# import os
# import time
# import logging
# import threading
# import subprocess
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler
# from watchdog.observers.polling import PollingObserver  # Use polling observer inste
# import sys
# WATCH_DIRECTORY = "/mnt/HabibData"
# RAW_SCRIPT = "/incremental_load"
# SIGNAL_DIRECTORY = "/mnt/inc_processed"  # Directory where the done.signal is placed after processing

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# def stream_output(pipe, log_fn):
#     for line in iter(pipe.readline, ''):
#         log_fn(line.strip())
#     pipe.close()
    
# # Wait for the file to stop updating for the given duration (in seconds)
# def is_file_stable(file_path, stable_duration=35):
#     """
#     Check if the file is stable (not modified) for the given duration.
#     Args:
#         file_path (str): Path to the file being monitored.
#         stable_duration (int): Duration (in seconds) the file should remain stable (not modified).

#     Returns:
#         bool: True if the file is stable for the given duration, False otherwise.
#     """
#     last_modified_time = os.path.getmtime(file_path)
#     start_time = time.time()

#     while time.time() - start_time < stable_duration:
#         time.sleep(1)  # Check every second
#         current_modified_time = os.path.getmtime(file_path)

#         if current_modified_time != last_modified_time:
#             # File has been modified, reset the check
#             last_modified_time = current_modified_time
#             start_time = time.time()  # Restart the timer

#     return True  # File has been stable for the duration

# class DirectoryWatcher(FileSystemEventHandler):
#     def on_created(self, event):
#         if event.is_directory:
#             return  # Ignore directory modifications

#         # Wait for the CSV file to stop updating before triggering the incremental load
#         if event.src_path.lower().endswith('.csv'):
#             csvpath = event.src_path
#             logging.info(f"[INFO] New CSV file detected: {csvpath}")
#             if 'Ageing' in csvpath:
#                 # Wait for the CSV file to stop updating before triggering the incremental load
#                 if is_file_stable(csvpath):
#                     self.RAW_SCRIPT(csvpath)
#                 else:
#                     logging.info(f"[INFO] File {event.src_path} is still being updated. Skipping trigger.")
#             else:
#                 logging.info(f"[INFO] File {event.src_path} name is unknown. Only Ageing files are accepted.")
#         else:
#             logging.info(f"[INFO] Non-CSV file detected, ignoring: {event.src_path}")
            
#     # def RAW_SCRIPT(self, new_file_path): #waits for load to complete to flush its output
#     #     # Use the same Python executable that's running this filewatcher
#     #     python_exec = sys.executable
        
#     #     script_command = [
#     #         python_exec,
#     #         RAW_SCRIPT,
#     #         new_file_path
#     #     ]
#     #     logging.info(f"Executing: {' '.join(script_command)}")
        
#     #     try:
#     #         result = subprocess.run(
#     #             script_command,
#     #             check=True,
#     #             stdout=subprocess.PIPE,
#     #             stderr=subprocess.PIPE,
#     #             text=True,
#     #         )
#     #         self.wait_for_completion()
#     #         logging.info("Script output:\n" + result.stdout)
#     #         if result.stderr:
#     #             logging.error("Script errors:\n" + result.stderr)
                
#     #     except subprocess.CalledProcessError as e:
#     #         logging.error(f"Script failed with code {e.returncode}")
#     #         logging.error("Error output:\n" + e.stderr)
#     #     except Exception as e:
#     #         logging.error(f"Unexpected error: {str(e)}")

#     def RAW_SCRIPT(self, new_file_path):
#         python_exec = sys.executable
#         script_command = [python_exec, RAW_SCRIPT, new_file_path]
#         logging.info(f"Executing: {' '.join(script_command)}")
    
#         try:
#             process = subprocess.Popen(
#                 script_command,
#                 stdout=subprocess.PIPE,
#                 stderr=subprocess.PIPE,
#                 text=True,
#                 bufsize=1,
#             )
    
#             threading.Thread(target=stream_output, args=(process.stdout, logging.info)).start()
#             threading.Thread(target=stream_output, args=(process.stderr, logging.error)).start()
    
#             return_code = process.wait()
#             if return_code != 0:
#                 logging.error(f"Script exited with code {return_code}")
#                 return
    
#             self.wait_for_completion()
    
#         except Exception as e:
#             logging.error(f"Unexpected error: {str(e)}")

#     def wait_for_completion(self):
#         signal_file = os.path.join(SIGNAL_DIRECTORY, "done.signal")
#         logging.info("[INFO] Waiting for incremental_load completion signal...")

#         timeout = 600  # Wait max 10 mins
#         start_time = time.time()

#         while time.time() - start_time < timeout:
#             if os.path.exists(signal_file):
#                 logging.info("[INFO] Incremental load completed! Acknowledging filewatcher.")
#                 os.remove(signal_file)  # Remove the signal file to reset for next cycle
#                 return
#             time.sleep(1)  # Check every second

#         logging.warning("[WARNING] Incremental load script did not send completion signal in time.")

# def start_watching():
#     logging.info("[INFO] Starting directory watcher...")
#     event_handler = DirectoryWatcher()
#     observer = PollingObserver()  # Use PollingObserver instead of Observer
#     observer.schedule(event_handler, WATCH_DIRECTORY, recursive=False)
#     observer.start()
#     try:
#         while True:
#             time.sleep(5)  # Prevent CPU overuse
#     except KeyboardInterrupt:
#         logging.info("\n[INFO] Stopping directory watcher...")
#         observer.stop()

#     observer.join()

# if __name__ == "__main__":
#     logging.info("[INFO] Filewatcher script is running...")
#     start_watching()

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
