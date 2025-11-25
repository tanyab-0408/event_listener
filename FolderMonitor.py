from watchdog.events import PatternMatchingEventHandler
import os

# --- MODULE 3: THE MONITOR ---
class FolderMonitor(PatternMatchingEventHandler):
    def __init__(self, state_manager, dispatcher, valid_ext, logger, watching_dir):
        super().__init__(
            patterns=['*.jpeg', '*.dav', '*.jpg', '*.png', '*.dav_'], 
            ignore_directories=True, 
            case_sensitive=False
        )
        self.state_manager = state_manager
        self.dispatcher = dispatcher
        self.valid_ext = valid_ext
        self.logger = logger
        self.watching_dir = watching_dir

    def handle_file(self, file_path):
        """Common logic for handling a detected file"""
        try:
            # 1. Check extension (double check to avoid .dav_)
            _, ext = os.path.splitext(file_path)
            if ext.lower() not in self.valid_ext:
                return

            # 2. Dispatch to Remote System
            success = self.dispatcher.send_task(file_path, self.watching_dir)
            # print('send to queue')

            # 3. Only update state if dispatch succeeded
            if success:
                mtime = os.path.getmtime(file_path)
                self.state_manager.update_state(file_path, mtime)

        except Exception as e:
            self.logger.error(f"Error handling file {file_path}: {e}")

    def on_moved(self, event):
        # Triggered when .dav_ becomes .dav
        if any(event.dest_path.lower().endswith(ext) for ext in self.valid_ext):
            self.logger.info(f"🔄 File Ready (Renamed): {os.path.basename(event.dest_path)}")
            self.handle_file(event.dest_path)

    def on_created(self, event):
        # Triggered for direct .jpg / .dav creation
        if any(event.src_path.lower().endswith(ext) for ext in self.valid_ext):
            self.logger.info(f"📁 File Ready (Created): {os.path.basename(event.src_path)}")
            self.handle_file(event.src_path)

    def run_catchup_scan(self, src_path):
        """
        Scans for files missed while the script was down.
        Uses os.walk but checks timestamps against the JSON state.
        """
        self.logger.info("🕵️  Starting Catch-up Scan...")
        count = 0
        
        for root, dirs, files in os.walk(src_path):
            # Get the last processed time for THIS specific folder
            # This handles the complex directory structure perfectly
            last_known_time = self.state_manager.get_last_timestamp(root)
            
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                if ext in self.valid_ext:
                    file_path = os.path.join(root, file)
                    try:
                        file_mtime = os.path.getmtime(file_path)
                        
                        # If file is newer than what we recorded for this folder
                        if file_mtime > last_known_time:
                            self.logger.info(f"🔎 Found missed file: {file}")
                            self.handle_file(file_path)
                            count += 1
                    except OSError:
                        pass # File might be locked/deleted

        self.logger.info(f"✅ Catch-up complete. Dispatched {count} missed files.")
