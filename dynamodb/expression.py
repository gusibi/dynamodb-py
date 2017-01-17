# -*- coding: utf-8 -*-

'''
DynamoDB KeyConditionExpression and FilterExpression
'''

from boto3.dynamodb.conditions import Key, Attr

from .errors import ValidationException

__all__ = ['Expression']


class Expression(object):

    def eq(self, value):
        # Creates a condition where the attribute is equal to the value.
        if self.hash_key or self.range_key:
            return Key(self.name).eq(value), True
        else:
            return Attr(self.name).eq(value), False

    def lt(self, value):
        # Creates a condition where the attribute is less than the value.
        if self.hash_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        if self.range_key:
            return Key(self.name).lt(value), True
        else:
            return Attr(self.name).lt(value), False

    def lte(self, value):
        # Creates a condition where the attribute is less than or
        # equal to the value.
        if self.hash_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        if self.range_key:
            return Key(self.name).lte(value), True
        else:
            return Attr(self.name).lte(value), False

    def gt(self, value):
        # Creates a condition where the attribute is greater than the value.
        if self.hash_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        if self.range_key:
            return Key(self.name).gt(value), True
        else:
            return Attr(self.name).gt(value), False

    def gte(self, value):
        # Creates a condition where the attribute is greater than or equal to
        # the value.
        if self.hash_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        if self.range_key:
            return Key(self.name).gte(value), True
        else:
            return Attr(self.name).gte(value), False

    def between(self, low_value, high_value):
        # Creates a condition where the attribute is greater than or equal to
        # the low value and less than or equal to the high value.
        if self.hash_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        if self.range_key:
            return Key(self.name).between(low_value, high_value), True
        else:
            return Attr(self.name).between(low_value, high_value), False

    def begins_with(self, value):
        # Creates a condition where the attribute begins with the value
        if self.hash_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        if self.range_key:
            return Key(self.name).begins_with(value), True
        else:
            return Attr(self.name).begins_with(value), False

    def ne(self, value):
        # Creates a condition where the attribute is not equal to the value
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return Attr(self.name).ne(value), False

    def is_in(self, value):
        # Creates a condition where the attribute is in the value
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return Attr(self.name).is_in(value)

    def contains(self, value):
        # Creates a condition where the attribute contains the value.
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return Attr(self.name).contains(value)

    def exists(self):
        # Creates a condition where the attribute exists.
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return Attr(self.name).exists()

    def not_exists(self):
        # Creates a condition where the attribute does not exists.
        if self.hash_key or self.range_key:
            # ValidationException
            raise ValidationException('Query key condition not supported')
        return Attr(self.name).not_exists()
