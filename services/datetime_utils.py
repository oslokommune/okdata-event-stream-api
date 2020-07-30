import pytz
import datetime


def utc_now_with_timezone():
    utc_now = datetime.datetime.utcnow()
    utc_now = utc_now.replace(tzinfo=pytz.utc)
    return utc_now
