#! -*- coding: utf-8 -*-

import pytz
from datetime import datetime, date, time


# http://stackoverflow.com/questions/24856643/unexpected-results-converting-timezones-in-python
def str_time(dt, fmt='%Y-%m-%d %H:%M:%S', with_timezone=False):
    """datetime to ISO 8601 str"""
    if not dt or not isinstance(dt, (datetime, date, time)):
        raise TypeError('dt should be date object, and not a %s' % type(dt))
    if not dt.tzinfo:
        # update dt with utc zone
        dt = pytz.utc.localize(dt)
    return dt.isoformat()


def str_to_time(dt_str):
    """str time to datetime"""
    formats = ['%Y%m%dT%H%M%S%z', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S%z']
    for format in formats:
        try:
            dt = datetime.strptime(dt_str, format)
            return dt
        except:
            raise TypeError('%s parsed error' % dt)


def get_attribute_type(attribute):
    from .fields import CharField, IntegerField
    print attribute, type(attribute)
    if isinstance(attribute, CharField):
        return 'S'
    elif isinstance(attribute, IntegerField):
        return 'N'
    else:
        raise TypeError('bad type')
