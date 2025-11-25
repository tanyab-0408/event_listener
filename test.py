import time
from datetime import datetime, timedelta

# 1. Get the datetime object for 10 minutes in the future
future_dt = datetime.now() + timedelta(minutes=10)

# 2. Convert that future time back into a Unix timestamp (float)
future_timestamp = future_dt.timestamp()

print(f"Current Time (Human): {datetime.now()}")
print(f"Future Time (Human): {future_dt}")
print(f"Future Timestamp (Paste into JSON): {future_timestamp}")