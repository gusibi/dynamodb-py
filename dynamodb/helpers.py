#! -*- coding: utf-8 -*-

import pytz
import dateutil.parser
from datetime import datetime, date, time


# http://stackoverflow.com/questions/24856643/unexpected-results-converting-timezones-in-python
def str_time(dt):
    """datetime to ISO 8601 str"""
    if not dt or not isinstance(dt, (datetime, date, time)):
        raise TypeError('dt should be date object, and not a %s' % type(dt))
    if not dt.tzinfo:
        # update dt with utc zone
        dt = pytz.utc.localize(dt)
    return dt.isoformat()


def str_date(dt):
    """datetime to ISO 8601 str"""
    if not dt or not isinstance(dt, (datetime, date, time)):
        raise TypeError('dt should be date object, and not a %s' % type(dt))
    if not dt.tzinfo:
        # update dt with utc zone
        dt = pytz.utc.localize(dt)
    return dt.date.isoformat()


def str_to_time(s):
    """str time to datetime"""
    try:
        dt = dateutil.parser.parse(s)
        return dt
    except ValueError:
        raise ValueError('%s parsed error' % s)


def get_attribute_type(attribute):
    from .fields import CharField, IntegerField
    if isinstance(attribute, CharField):
        return 'S'
    elif isinstance(attribute, IntegerField):
        return 'N'
    else:
        raise TypeError('bad type')


def get_items_for_storage(model_class, items):
    results = []
    for item in items:
        instance = model_class(**item)
        results.append(instance.item)
    return results