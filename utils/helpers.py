from datetime import datetime


def get_time_in_iso():
    return datetime.now().isoformat()


def time_to_datetime_in_ms(timestamp):
    return datetime.fromtimestamp(timestamp / 1e3)


def time_in_ms_to_iso(timestamp):
    return datetime.fromtimestamp(timestamp / 1e3).isoformat()
