import watchdog.events
import watchdog.observers
import time
import os

class Handler(watchdog.events.PatternMatchingEventHandler):
    def __init__(self):
        # Watch for both temporary and final extensions
        watchdog.events.PatternMatchingEventHandler.__init__(
            self, 
            patterns=['*.jpeg', '*.dav', '*.jpg', '*.png', '*.dav_'],  # Include temp extension
            ignore_directories=True, 
            case_sensitive=False
        )
        print('watching...........')

    def on_created(self, event):
        print(f"📁 Created: {os.path.basename(event.src_path)}")
        
    def on_moved(self, event):
        # This is triggered when file is renamed from .dav_ to .dav
        print(f"🔄 Moved/Renamed: {os.path.basename(event.src_path)} -> {os.path.basename(event.dest_path)}")
        
        # Check if this is a rename from temp to final extension
        if event.dest_path.lower().endswith(('.dav', '.jpg', '.jpeg', '.png')):
            print(f"✅ File ready for processing: {event.dest_path}")
            # Your file processing logic here
            self.process_file(event.dest_path)

    def process_file(self, file_path):
        """Process the completed file"""
        try:
            file_size = os.path.getsize(file_path)
            print(f"🎯 Processing: {os.path.basename(file_path)} (size: {file_size} bytes)")
            # Add your actual file processing code here
            
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")

if __name__ == "__main__":
    src_path = r"C:\SFTP_Root\Storepulse3"
    event_handler = Handler()
    observer = watchdog.observers.Observer()
    observer.schedule(event_handler, path=src_path, recursive=True)
    observer.start()
    print(f"Monitoring: {src_path}")
    print("Waiting for file events...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("Stopping monitor...")
    observer.join()
