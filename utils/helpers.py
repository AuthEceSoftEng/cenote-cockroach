from datetime import datetime


def get_time_in_iso():
    return datetime.utcnow().isoformat()


def time_in_ms_to_iso(timestamp):
    return datetime.fromtimestamp(timestamp / 1e3).isoformat()
