# -*- coding: utf-8 -*-

'''
DynamoDB KeyConditionExpression and FilterExpression
'''

from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

from .errors import ValidationException

__all__ = ['Expression']


class Expression(object):

    def _expression_func(self, op, *args, **kwargs):
        # for use by index ... bad
        print args, self.use_decimal_types
        if self.use_decimal_types:
            args = map(lambda x: Decimal(str(x)), args)
        self.op = op
        self.express_args = args
        use_key = kwargs.get('use_key', False)
        if self.hash_key and op != 'eq':
            raise ValidationException('Query key condition not supported')
        elif self.hash_key or self.range_key or use_key:
            use_key = True
            func = getattr(Key(self.name), op, None)
        else:
            func = getattr(Attr(self.name), op, None)
        if not func:
            raise ValidationException('Query key condition not supported')
        return self, func(*args), use_key

    def eq(self, value):
        # Creates a condition where the attribute is equal to the value.
        return self._expression_func('eq', value)

    def lt(self, value):
        # Creates a condition where the attribute is less than the value.
        return self._expression_func('lt', value)

    def lte(self, value):
        # Creates a condition where the attribute is less than or
        # equal to the value.
        return self._expression_func('lte', value)

    def gt(self, value):
        # Creates a condition where the attribute is greater than the value.
        return self._expression_func('gt', value)

    def gte(self, value):
        # Creates a condition where the attribute is greater than or equal to
        # the value.
        return self._expression_func('gte', value)

    def between(self, low_value, high_value):
        # Creates a condition where the attribute is greater than or equal to
        # the low value and less than or equal to the high value.
        return self._expression_func('between', low_value, high_value)

    def begins_with(self, value):
        # Creates a condition where the attribute begins with the value
        return self._expression_func('begins_with', value)

    def ne(self, value):
        # Creates a condition where the attribute is not equal to the value
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return self.name, Attr(self.name).ne(value), False

    def is_in(self, value):
        # Creates a condition where the attribute is in the value
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return self.name, Attr(self.name).is_in(value), False

    def contains(self, value):
        # Creates a condition where the attribute contains the value.
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return self.name, Attr(self.name).contains(value), False

    def exists(self):
        # Creates a condition where the attribute exists.
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return self.name, Attr(self.name).exists(), False

    def not_exists(self):
        # Creates a condition where the attribute does not exists.
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return self.name, Attr(self.name).not_exists(), False
