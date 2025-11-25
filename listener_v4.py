import sys
import time
import os
import json
import argparse
import logging
from watchdog.observers import Observer
from StateManager import StateManager
from FolderMonitor import FolderMonitor
from RemoteDispatcher_v2 import RemoteDispatcher
# Register the signal handler for Ctrl+C and termination signals
import signal
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
# Configure your logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants from .env
RABBIT_HOST = os.getenv('RABBIT_HOST', 'localhost')
RABBIT_PORT = int(os.getenv('RABBIT_PORT', 5672))
RABBIT_USER = os.getenv('RABBIT_USER', 'guest')
RABBIT_PASS = os.getenv('RABBIT_PASS', 'guest')
RABBIT_VHOST = os.getenv('RABBIT_VHOST', '/')
EXCHANGE_NAME = os.getenv('RABBIT_EXCHANGE', 'dahua_video_exchange')
ROUTING_KEY_VIDEO = os.getenv('RABBIT_ROUTING_KEY_VIDEO', 'video_processing')
ROUTING_KEY_IMAGE = os.getenv('RABBIT_ROUTING_KEY_IMAGE', 'image_processing')
STATE_FILE = os.getenv('STATE_FILE', 'monitor_state.json')
SERVER_ID = os.getenv('SERVER_ID', 'no id')

rabbit_dict = {
     'host': RABBIT_HOST,
     'port':RABBIT_PORT,
     'user':RABBIT_USER,
     'pass':RABBIT_PASS,
     'vhost':RABBIT_VHOST,
     'exchange':EXCHANGE_NAME,
     'routing_key_vid':ROUTING_KEY_VIDEO,
     'routing_key_img':ROUTING_KEY_IMAGE,
     'server_id':SERVER_ID
}

VALID_EXTENSIONS = {'.dav', '.jpg', '.jpeg', '.png'}

def initialize_state_manager(root_path):
    """Generates a unique state file name for the given root path."""
    
    # 1. Clean the path string (remove invalid file characters)
    #    Example: C:\SFTP_Root\BatchA -> C_SFTP_Root_BatchA
    sanitized_path = root_path.replace(os.sep, '_').replace(':', '')

    # 2. Define the unique state file name
    unique_state_file = f"state_{sanitized_path[:50]}.json" 
    
    # Ensure the state file is placed in the same directory as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_state_path = os.path.join(script_dir, unique_state_file)
    
    logger.info(f"💾 Using unique state file: {full_state_path}")
    return StateManager(full_state_path, root_path, logger)


# --- MAIN EXECUTION ---
# --- MAIN EXECUTION ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dahua RabbitMQ Monitor")
    parser.add_argument("source_paths", nargs='+', help="List of directories to monitor")
    args = parser.parse_args()

    # Validate paths
    for path in args.source_paths:
        if not os.path.exists(path):
            logger.error(f"❌ Path does not exist: {path}")
            sys.exit(1)

    # --- 1. INITIALIZE GLOBALS BEFORE LOGIC ---
    # We init these here so 'graceful_exit' can see them
    dispatcher = RemoteDispatcher(rabbit_dict)
    observer = Observer()
    active_managers = []

    # --- 2. DEFINE EXIT HANDLER EARLY ---
    # This must be defined BEFORE we start doing work
    def graceful_exit(signum=None, frame=None):
        logger.info("\n🛑 Stop signal received. Flushing ALL states...")
        
        # Stop the observer if it's running
        try:
            if observer.is_alive():
                observer.stop()
                observer.join()
        except Exception as e:
            logger.error(f"Error stopping observer: {e}")
        
        # Save all states
        if active_managers:
            for mgr in active_managers:
                try:
                    mgr.flush_state_to_disk()
                except Exception as e:
                    logger.error(f"Error saving state: {e}")
        
        # Close connection
        try:
            dispatcher.close()
        except Exception:
            pass
            
        logger.info("👋 Exited.")
        sys.exit(0)

    # --- 3. REGISTER SIGNALS ---
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    # --- 4. START LOGIC (Try Block) ---
    try:
        for source_path in args.source_paths:
            logger.info(f"🔧 Setting up monitor for: {source_path}")

            # A. Create State Manager
            state_mgr = initialize_state_manager(source_path)
            state_mgr.prune_old_keys(days_to_keep=7)
            active_managers.append(state_mgr) 

            # B. Create Monitor
            monitor = FolderMonitor(state_mgr, dispatcher, VALID_EXTENSIONS,logger, source_path)

            # C. Run Catch-up 
            # (If you Ctrl+C here now, graceful_exit IS defined, so it works!)
            monitor.run_catchup_scan(source_path)

            # D. Schedule Observer
            observer.schedule(monitor, path=source_path, recursive=True)

        # Start the master observer
        observer.start()
        logger.info(f"👀 Monitoring active on {len(args.source_paths)} directories.")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        # Now this will work because the function is defined above
        graceful_exit()
    except Exception as e:
        logger.error(f"🔥 Unexpected Runtime Error: {e}")
        graceful_exit()

# # --- MAIN EXECUTION ---
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Dahua RabbitMQ Monitor")
#     parser.add_argument("source_path", help="Directory to monitor")
#     args = parser.parse_args()

#     if not os.path.exists(args.source_path):
#         logger.error(f"Source path '{args.source_path}' does not exist.")
#         sys.exit(1)

#     # 1. Initialize Modules

#     # state_mgr = StateManager("monitor_state.json", args.source_path, logger)
#     state_mgr = initialize_state_manager(args.source_path)
    
#     # Clean up very old keys from JSON to save memory
#     state_mgr.prune_old_keys(days_to_keep=7)

#     # Initialize RabbitMQ Dispatcher
#     dispatcher = RemoteDispatcher(rabbit_dict)

#     # Setup Monitor
#     event_handler = FolderMonitor(state_mgr, dispatcher, VALID_EXTENSIONS, logger, args.source_path)
    
#     observer = Observer()
#     observer.schedule(event_handler, path=args.source_path, recursive=True)

#     # 2. Signal Handling Function
#     def graceful_exit(signum=None, frame=None):
#         logger.info("\n🛑 Stop signal received. Flushing state...")
#         try:
#             observer.stop() # Stop watching new files
#             observer.join() # Wait for observer to finish
#         except:
#             pass
            
#         # CRITICAL: Save the state
#         state_mgr.flush_state_to_disk() 
        
#         # Close MQ connection
#         dispatcher.close()
#         sys.exit(0)

#     # Register signals
#     signal.signal(signal.SIGINT, graceful_exit)
#     signal.signal(signal.SIGTERM, graceful_exit)

#     # 3. Execution Block (The Fix)
#     try:
#         # --- MOVED INSIDE TRY BLOCK ---
#         # Now if you Ctrl+C here, it goes to 'except KeyboardInterrupt'
#         event_handler.run_catchup_scan(args.source_path)
        
#         # Start Watchdog
#         observer.start()
#         logger.info(f"👀 Monitoring active: {args.source_path}")

#         # Keep alive loop
#         while True:
#             time.sleep(1)

#     except KeyboardInterrupt:
#         # This catches Ctrl+C during catch-up or main loop
#         graceful_exit()
#     except Exception as e:
#         logger.error(f"🔥 Unexpected Runtime Error: {e}")
#         graceful_exit()


