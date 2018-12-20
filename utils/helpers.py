import time
from datetime import datetime


def get_time_in_ms():
    
    return int(round(time.time() * 1000))

def time_to_datetime_in_ms(timestamp):
    
    return datetime.fromtimestamp(timestamp / 1e3)