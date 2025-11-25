import os
import json
from datetime import datetime, timedelta

class StateManager:
    """
    Manages persistence of the last processed timestamp per folder.
    Uses relative paths to save memory.
    """
    def __init__(self, state_file, root_dir, logger):
        self.state_file = state_file
        self.root_dir = root_dir
        self.state = {}
        self.logger = logger
        self.load_state()

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    self.state = json.load(f)
                self.logger.info(f"📖 State loaded. Tracking {len(self.state)} directories.")
            except Exception as e:
                self.logger.error(f"⚠️ Corrupt state file, starting fresh: {e}")

    def get_last_timestamp(self, file_dir):
        """Returns the last timestamp for a specific directory (relative path)."""
        rel_path = os.path.relpath(file_dir, self.root_dir)
        return self.state.get(rel_path, 0.0)

    def update_state(self, file_path, timestamp):
        """Updates the timestamp for the folder containing the file."""
        file_dir = os.path.dirname(file_path)
        rel_path = os.path.relpath(file_dir, self.root_dir)
        
        # Only update if newer to prevent regression during async processing
        if timestamp > self.state.get(rel_path, 0.0):
            self.state[rel_path] = timestamp
            # self._save_state()

    def _save_state(self):
        """Atomic write to JSON"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            self.logger.error(f"❌ Failed to save state: {e}")

    def flush_state_to_disk(self):
        """Called only upon program exit."""
        self.logger.info("💾 Flushing state to disk...")
        self._save_state()

    def prune_old_keys(self, days_to_keep=7):
        """Maintenance: Remove folders older than X days from JSON to save memory."""
        cutoff = (datetime.now() - timedelta(days=days_to_keep)).timestamp()
        keys_to_remove = [k for k, v in self.state.items() if v < cutoff]
        for k in keys_to_remove:
            del self.state[k]
        if keys_to_remove:
            self._save_state()
            self.logger.info(f"🧹 Pruned {len(keys_to_remove)} old directories from state.")

