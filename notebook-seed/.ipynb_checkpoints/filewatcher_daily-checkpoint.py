import os
import logging
import subprocess
import schedule
import time

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

WATCH_DIRECTORY = "/mnt/HabibData"
TRIGGER_SCRIPT = "/incremental_load"
SIGNAL_DIRECTORY = "/mnt/inc_processed"  # Directory where the done.signal is placed after processing

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DirectoryWatcher(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return  # Ignore directory modifications

        logging.info(f"[INFO] New CSV file detected: {event.src_path}")
        self.trigger_script(event.src_path)

    def trigger_script(self, new_file_path):
        script_command = ["python", TRIGGER_SCRIPT, new_file_path]
        logging.info(f"[INFO] Triggering script: {' '.join(script_command)}")

        try:
            result = subprocess.run(script_command, capture_output=True, text=True, check=True)
            logging.info(f"[INFO] Script output:\n{result.stdout}")
            self.wait_for_completion()

        except subprocess.CalledProcessError as e:
            logging.error(f"[ERROR] Script failed:\n{e.stderr}")

    def wait_for_completion(self):
        signal_file = os.path.join(SIGNAL_DIRECTORY, "done.signal")        
        logging.info("[INFO] Waiting for incremental_load completion signal...")

        timeout = 60  # Wait max 60 seconds
        start_time = time.time()

        while time.time() - start_time < timeout:
            if os.path.exists(signal_file):
                logging.info("[INFO] Incremental load completed! Acknowledging filewatcher.")
                os.remove(signal_file)  # Remove the signal file to reset for next cycle
                return
            time.sleep(1)  # Check every second
        
        logging.warning("[WARNING] Incremental load script did not send completion signal in time.")

def check_directory():
    logging.info("[INFO] Checking the directory once a day...")
    event_handler = DirectoryWatcher()
    observer = PollingObserver()  # Use PollingObserver instead of Observer
    observer.schedule(event_handler, WATCH_DIRECTORY, recursive=False)
    observer.start(
