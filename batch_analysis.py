import pandas as pd
from datetime import timedelta

# Simulate loading a static daily dump of user events
# In a real scenario, this would read from S3/HDFS/Snowflake
data = [
    {"user_id": "user_123", "event_type": "view", "timestamp": "2023-10-27T10:05:00Z"},
    {"user_id": "user_123", "event_type": "click", "timestamp": "2023-10-27T10:15:00Z"},
    {"user_id": "user_123", "event_type": "view", "timestamp": "2023-10-27T10:25:00Z"},
    # LATE EVENT (Arrives in the system at 11:10, but timestamp is 10:45)
    {"user_id": "user_123", "event_type": "click", "timestamp": "2023-10-27T10:45:00Z"},
]

df = pd.DataFrame(data)
df['timestamp'] = pd.to_datetime(df['timestamp'])

print("--- Batch Feature Computation (End of Day) ---")

# Batch grouping: Group by 1-hour tumbling windows
df.set_index('timestamp', inplace=True)
grouped = df.groupby(['user_id', pd.Grouper(freq='1H')])

for (user, window), group in grouped:
    total_events = len(group)
    clicks = len(group[group['event_type'] == 'click'])
    click_rate = clicks / total_events if total_events > 0 else 0
    
    print(f"User: {user} | Window: {window.time()} - {(window + timedelta(hours=1)).time()}")
    print(f"Total Events: {total_events} | Clicks: {clicks}")
    print(f"Batch Click Rate: {click_rate:.2f}\n")