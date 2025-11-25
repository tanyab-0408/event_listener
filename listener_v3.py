import sys
import time
import os
import json
import argparse
import logging
from watchdog.observers import Observer
from StateManager import StateManager
from FolderMonitor import FolderMonitor
from RemoteDispatcher import RemoteDispatcher
# Register the signal handler for Ctrl+C and termination signals
import signal


# --- CONFIGURATION ---
# Configure your logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

VALID_EXTENSIONS = {'.dav', '.jpg', '.jpeg', '.png'}

def signal_handler(signum, frame):
        """Function to be called when a SIGINT or SIGTERM is received."""
        logger.info("🛑 Stop signal received. Initiating graceful shutdown...")
        # 1. Stop the watchdog observer first
        observer.stop()
        # 2. Flush the in-memory state to disk
        state_mgr.flush_state_to_disk()
        sys.exit(0) # Exit the program cleanly

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# def main():
# --- MAIN EXECUTION ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Dahua Monitor")
    parser.add_argument("source_path", help="Root directory to monitor")
    args = parser.parse_args()

    if not os.path.exists(args.source_path):
        logger.error("Source path does not exist.")
        sys.exit(1)

    # Initialize Modules
    state_mgr = StateManager("monitor_state.json", args.source_path, logger)
    
    # Clean up very old keys from JSON to save memory
    state_mgr.prune_old_keys(days_to_keep=7)

    # Setup Dispatcher (Add your RabbitMQ/API URL here)
    # dispatcher = RemoteDispatcher(remote_url="http://worker-node:5000/process")
    RABBIT_URL = "amqp://guest:guest@localhost:5672/%2F" 
    EXCHANGE = "dahua_video_exchange"
    ROUTING_KEY = "video_processing"

    # Initialize RabbitMQ Dispatcher
    dispatcher = RemoteDispatcher(RABBIT_URL, EXCHANGE, ROUTING_KEY)

    # Setup Monitor
    event_handler = FolderMonitor(state_mgr, dispatcher, VALID_EXTENSIONS, logger)
    
    # 1. Run Catch-up FIRST
    event_handler.run_catchup_scan(args.source_path)

    # 2. Start Real-time Watchdog
    observer = Observer()
    observer.schedule(event_handler, path=args.source_path, recursive=True)
    observer.start()
    logger.info(f"👀 Real-time monitoring active on: {args.source_path}")
    # observer.join()
    

    try:
        # Keep the main thread alive until a signal is received
        while True:
            time.sleep(1)
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}")
    finally:
        # This block executes if the loop breaks or an exception occurs
        observer.join()
        logger.info("🛑 Monitor stopping...👋 Exited cleanly.")

# # --- MAIN EXECUTION ---
# if __name__ == "__main__":
#     main()
# space available, services running (docker container running) on every system, frs server: 4 services running 