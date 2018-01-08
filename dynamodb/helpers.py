#! -*- coding: utf-8 -*-
try:
    import cPickle as pickle
except ImportError:
    import pickle

from functools import wraps

import six
import pytz
import dateutil.parser
from decimal import Decimal
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
    if not s:
        return None
    try:
        dt = dateutil.parser.parse(s)
        return dt
    except ValueError:
        raise ValueError('%s parsed error' % s)


def date2timestamp(dt):
    # datetime to timestamp
    import time
    if not isinstance(dt, datetime):
        return dt
    timestamp = time.mktime(dt.timetuple()) + dt.microsecond/1e6
    return timestamp


def timestamp2date(timestamp):
    if not isinstance(timestamp, (int, float)):
        return timestamp
    date = datetime.utcfromtimestamp(timestamp)
    utcdt = pytz.utc.localize(date)
    return utcdt


class Promise(object):
    """
    This is just a base class for the proxy class created in
    the closure of the lazy function. It can be used to recognize
    promises in code.
    """
    pass


class _UnicodeDecodeError(UnicodeDecodeError):
    def __init__(self, obj, *args):
        self.obj = obj
        UnicodeDecodeError.__init__(self, *args)

    def __str__(self):
        original = UnicodeDecodeError.__str__(self)
        return '%s. You passed in %r (%s)' % (original, self.obj,
                                              type(self.obj))


def smart_text(s, encoding='utf-8', strings_only=False, errors='strict'):
    """
    Returns a text object representing 's' -- unicode on Python 2 and str on
    Python 3. Treats bytestrings using the 'encoding' codec.
    If strings_only is True, don't convert (some) non-string-like objects.
    """
    if isinstance(s, Promise):
        # The input is the result of a gettext_lazy() call.
        return s
    return force_text(s, encoding, strings_only, errors)


_PROTECTED_TYPES = six.integer_types + (type(None), float, Decimal,
                                        datetime, date, time)


def is_protected_type(obj):
    """Determine if the object instance is of a protected type.
    Objects of protected types are preserved as-is when passed to
    force_text(strings_only=True).
    """
    return isinstance(obj, _PROTECTED_TYPES)


def force_text(s, encoding='utf-8', strings_only=False, errors='strict'):
    """
    Similar to smart_text, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.
    If strings_only is True, don't convert (some) non-string-like objects.
    """
    # Handle the common case first for performance reasons.
    if issubclass(type(s), six.text_type):
        return s
    if strings_only and is_protected_type(s):
        return s
    try:
        if not issubclass(type(s), six.string_types):
            if six.PY3:
                if isinstance(s, bytes):
                    s = six.text_type(s, encoding, errors)
                else:
                    s = six.text_type(s)
            elif hasattr(s, '__unicode__'):
                s = six.text_type(s)
            else:
                s = six.text_type(bytes(s), encoding, errors)
        else:
            # Note: We use .decode() here, instead of six.text_type(s, encoding,
            # errors), so that if s is a SafeBytes, it ends up being a
            # SafeText at the end.
            s = s.decode(encoding, errors)
    except UnicodeDecodeError as e:
        if not isinstance(s, Exception):
            raise _UnicodeDecodeError(s, *e.args)
        else:
            # If we get to here, the caller has passed in an Exception
            # subclass populated with non-ASCII bytestring data without a
            # working unicode method. Try to handle this without raising a
            # further exception by individually forcing the exception args
            # to unicode.
            s = ' '.join(force_text(arg, encoding, strings_only, errors)
                         for arg in s)
    return s


def smart_bytes(s, encoding='utf-8', strings_only=False, errors='strict'):
    """
    Returns a bytestring version of 's', encoded as specified in 'encoding'.
    If strings_only is True, don't convert (some) non-string-like objects.
    """
    if isinstance(s, Promise):
        # The input is the result of a gettext_lazy() call.
        return s
    return force_bytes(s, encoding, strings_only, errors)


def force_bytes(s, encoding='utf-8', strings_only=False, errors='strict'):
    """
    Similar to smart_bytes, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.
    If strings_only is True, don't convert (some) non-string-like objects.
    """
    # Handle the common case first for performance reasons.
    if isinstance(s, bytes):
        if encoding == 'utf-8':
            return s
        else:
            return s.decode('utf-8', errors).encode(encoding, errors)
    if strings_only and is_protected_type(s):
        return s
    if isinstance(s, Promise):
        return six.text_type(s).encode(encoding, errors)
    if not isinstance(s, six.string_types):
        try:
            if six.PY3:
                return six.text_type(s).encode(encoding)
            else:
                return bytes(s)
        except UnicodeEncodeError:
            if isinstance(s, Exception):
                # An Exception subclass containing non-ASCII data that doesn't
                # know how to print itself properly. We shouldn't raise a
                # further exception.
                return b' '.join(force_bytes(arg, encoding,
                                             strings_only, errors)
                                 for arg in s)
            return six.text_type(s).encode(encoding, errors)
    else:
        return s.encode(encoding, errors)

if six.PY3:
    smart_str = smart_text
    force_str = force_text
else:
    smart_str = smart_bytes
    force_str = force_bytes
    # backwards compatibility for Python 2
    smart_unicode = smart_text
    force_unicode = force_text

smart_str.__doc__ = """
Apply smart_text in Python 3 and smart_bytes in Python 2.
This is suitable for writing to sys.stdout (for instance).
"""

force_str.__doc__ = """
Apply force_text in Python 3 and force_bytes in Python 2.
"""


def cache_for(duration):
    def deco(func):
        @wraps(func)
        def fn(*args, **kwargs):
            import time
            all_args = []
            all_args.append(args)
            key = pickle.dumps((all_args, kwargs))
            value, expire = func.func_dict.get(key, (None, None))
            now = int(time.time())
            if value is not None and expire > now:
                return value
            value = func(*args, **kwargs)
            func.func_dict[key] = (value, int(time.time()) + duration)
            return value
        return fn
    return deco


def get_attribute_type(attribute):
    from .fields import CharField, IntegerField, DateTimeField, FloatField, TimeField
    if isinstance(attribute, (CharField, DateTimeField)):
        return 'S'
    elif isinstance(attribute, (IntegerField, FloatField, TimeField)):
        return 'N'
    else:
        raise TypeError('%s bad type' % attribute.name)


def get_items_for_storage(model_class, items):
    results = []
    for item in items:
        instance = model_class(**item)
        results.append(instance.item)
    return results
