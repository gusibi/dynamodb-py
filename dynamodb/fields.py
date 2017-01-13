#! -*- coding: utf-8 -*-
'''
dynamodb model fields
'''
from __future__ import unicode_literals

import json
import decimal
from datetime import datetime, date, timedelta

from .errors import FieldValidationError
from .helpers import str_time


__all__ = ['Attribute', 'CharField', 'IntegerField', 'FloatField',
           'DateTimeField', 'DateField', 'TimeDeltaField', 'BooleanField']

# TODO
# 完善类型
# 完成表操作
# 完成项目create get batch_write batch_get batch_put delete
# 完成index
# 完成 query scan


class DecimalEncoder(json.JSONEncoder):
    # Helper class to convert a DynamoDB item to JSON.

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class Attribute(object):
    """Defines an attribute of the model.
    The attribute accepts strings and are stored in Redis as
    they are - strings.
    Options
        name         -- alternate name of the attribute. This will be used
                        as the key to use when interacting with Redis.
        hash_key     -- is hash_key. Table primary key.
        range_key    -- is range_key. Table primary key.
        indexed      -- Index this attribute. Unindexed attributes cannot
                        be used in queries. Default: False.
        unique       -- validates the uniqueness of the value of the
                        attribute.
        validator    -- a callable that can validate the value of the
                        attribute.
        default      -- Initial value of the attribute.
    """
    def __init__(self,
                 name=None,
                 hash_key=False,
                 range_key=False,
                 indexed=False,
                 required=False,
                 validator=None,
                 unique=False,
                 default=None):
        self.name = name
        self.unique = unique
        self.indexed = indexed
        self.default = default
        self.required = required
        self.validator = validator
        self.hash_key = hash_key
        self.range_key = range_key

    def __get__(self, instance, owner):
        try:
            return getattr(instance, '_' + self.name)
        except AttributeError:
            if callable(self.default):
                default = self.default()
            else:
                default = self.default
            self.__set__(instance, default)
            return default

    def __set__(self, instance, value):
        setattr(instance, '_' + self.name, value)

    def typecast_for_read(self, value):
        """Typecasts the value for reading from DynamoDB."""
        return value

    def typecast_for_storage(self, value):
        """Typecasts the value for storing to DynamoDB."""
        # default store unicode
        try:
            return unicode(value)
        except UnicodeError:
            return value.decode('utf-8')

    def value_type(self):
        return unicode

    def acceptable_types(self):
        return basestring

    def validate(self, instance):
        val = getattr(instance, self.name)
        errors = []
        # type_validation
        if val is not None and not isinstance(val, self.acceptable_types()):
            errors.append((self.name, 'Bad type',))
        # validate first standard stuff
        if self.required:
            if val is None or not unicode(val).strip():
                errors.append((self.name, 'required'))
        # validate using validator
        if self.validator:
            r = self.validator(self.name, val)
            if r:
                errors.extend(r)
        if errors:
            raise FieldValidationError(errors)


class CharField(Attribute):

    def __init__(self, max_length=255, **kwargs):
        super(CharField, self).__init__(**kwargs)
        self.max_length = max_length

    def typecast_for_read(self, value):
        if value == 'None':
            return ''
        return value.decode('utf-8')

    def typecast_for_storage(self, value):
        """Typecasts the value for storing to Redis."""
        if value is None:
            return ''
        try:
            return unicode(value)
        except UnicodeError:
            return value.decode('utf-8')

    def validate(self, instance):
        errors = []
        try:
            super(CharField, self).validate(instance)
        except FieldValidationError as err:
            errors.extend(err.errors)

        val = getattr(instance, self.name)

        if val and len(val) > self.max_length:
            errors.append((self.name, 'exceeds max length'))

        if errors:
            raise FieldValidationError(errors)


class IntegerField(Attribute):

    def typecast_for_read(self, value):
        return int(value)

    def typecast_for_storage(self, value):
        if value is None:
            return 0
        return int(value)

    def value_type(self):
        return int

    def acceptable_types(self):
        return (int, long)

    def validate(self, instance):
        errors = []
        try:
            super(IntegerField, self).validate(instance)
        except FieldValidationError as err:
            errors.extend(err.errors)

        val = getattr(instance, self.name)

        if val and not isinstance(val, int):
            errors.append((self.name, 'type error, need integer'))

        if errors:
            raise FieldValidationError(errors)


class FloatField(Attribute):

    def typecast_for_read(self, value):
        return float(value)

    def typecast_for_storage(self, value):
        if value is None:
            value = 0
        return decimal.Decimal(str(value))

    def value_type(self):
        return float

    def acceptable_types(self):
        return self.value_type()

    def validate(self, instance):
        errors = []
        try:
            super(FloatField, self).validate(instance)
        except FieldValidationError as err:
            errors.extend(err.errors)

        val = getattr(instance, self.name)

        if val and not isinstance(val, float):
            errors.append((self.name, 'type error, need float'))

        if errors:
            raise FieldValidationError(errors)


class BooleanField(Attribute):

    def typecast_for_read(self, value):
        return bool(int(value))

    def typecast_for_storage(self, value):
        if value is None:
            return False
        return value

    def value_type(self):
        return bool

    def acceptable_types(self):
        return self.value_type()


class DateTimeField(Attribute):

    def __init__(self, auto_now=False, auto_now_add=False, **kwargs):
        super(DateTimeField, self).__init__(**kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add

    def typecast_for_read(self, value):
        try:
            # We load as if the timestampe was naive
            dt = datetime.fromtimestamp(float(value), tzutc())
            # And gently override (ie: not convert) to the TZ to UTC
            return dt
        except TypeError:
            return None
        except ValueError:
            return None

    def typecast_for_storage(self, value):
        if not isinstance(value, date):
            raise TypeError("%s should be date object, and not a %s" %
                            (self.name, type(value)))
        if value is None:
            return None
        return str_time(value)

    def value_type(self):
        return datetime

    def acceptable_types(self):
        return self.value_type()


class DateField(Attribute):

    def __init__(self, auto_now=False, auto_now_add=False, **kwargs):
        super(DateField, self).__init__(**kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add

    def typecast_for_read(self, value):
        try:
            # We load as if it is UTC time
            dt = date.fromtimestamp(float(value))
            # And assign (ie: not convert) the UTC TimeZone
            return dt
        except TypeError:
            return None
        except ValueError:
            return None

    def typecast_for_storage(self, value):
        if not isinstance(value, date):
            raise TypeError("%s should be date object, and not a %s" %
                            (self.name, type(value)))
        if value is None:
            return None
        return str_time(value)

    def value_type(self):
        return date

    def acceptable_types(self):
        return self.value_type()


class TimeDeltaField(Attribute):

    def __init__(self, **kwargs):
        super(TimeDeltaField, self).__init__(**kwargs)

    if hasattr(timedelta, "totals_seconds"):
        def _total_seconds(self, td):
            return td.total_seconds
    else:
        def _total_seconds(self, td):
            return (td.microseconds + 0.0 +
                    (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6

    def typecast_for_read(self, value):
        try:
            # We load as if it is UTC time
            if value is None:
                value = 0.
            td = timedelta(seconds=float(value))
            return td
        except TypeError:
            return None
        except ValueError:
            return None

    def typecast_for_storage(self, value):
        if not isinstance(value, timedelta):
            raise TypeError("%s should be timedelta object, and not a %s" %
                            (self.name, type(value)))
        if value is None:
            return None

        return "%d" % self._total_seconds(value)

    def value_type(self):
        return timedelta

    def acceptable_types(self):
        return self.value_type()


class DocumentField(Attribute):

    def __init__(self, **kwargs):
        super(DocumentField, self).__init__(**kwargs)
