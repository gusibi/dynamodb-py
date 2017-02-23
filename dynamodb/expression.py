# -*- coding: utf-8 -*-

'''
DynamoDB KeyConditionExpression and FilterExpression
'''
from __future__ import print_function

from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

from .errors import ValidationException
from .helpers import smart_unicode

__all__ = ['Expression']


class Expression(object):

    def set(self, value,
            set_path=None,
            attr_label=None,
            if_not_exists=None,
            list_append=None):
        '''
        parameters:
            - value: value
            - set_path: attr path if not use attr name
            - attr_label: string attr label ex: label=':p'
            - if_not_exists: string path ex: Price
            - list_append: (tuple) path, index
            ex: (#pr.FiveStar, -1) to last
                (#pr.FiveStar, 0) to first
        examples:
            Test(realname='gs', score=100).update(Test.order_score.set(100))
            Test(realname='gs', score=100).update(
                Test.order_score.set(5, label=':p')
            Test(realname='gs', score=100).update(
                Test.order_score.set(100, is_not_exists=('order_score', 50)))
            Test(realname='gs', score=100).update(
                Test.ids.set(100, list_append=('ids')))
            or
            Test(realname='gs', score=100).update(
                Test.ids.list_append(100))

        return exp, {label: value}
        '''
        path = attr_label or self.name
        label = ":{name}".format(name=path)
        attr_name = "#{name}".format(name=path)
        # ExpressionAttributeValues
        if isinstance(value, float) or self.use_decimal_types:
            value = Decimal(str(value))
        eav = {label: value}
        ean = {}
        if if_not_exists:
            no_path, operand = if_not_exists, value
            if isinstance(operand, float):
                operand = Decimal(str(operand))
            eav[label] = operand
            ean[attr_name] = path
            exp = '{name} = if_not_exists({path}, {label})'.format(
                name=attr_name, path=no_path, label=label)
        elif list_append:
            list_path, index = list_append
            if index == 0:
                exp = "{path} = list_append({label}, {path})".format(
                    path=list_path, label=label)
            elif index == -1:
                exp = "{path} = list_append({path}, {label})".format(
                    path=list_path, label=label)
            else:
                raise ValidationException('index error')
        else:
            path = set_path or self.name
            attr_name = "#{name}".format(name=attr_label or path)
            ean[attr_name] = path
            exp = '{path} = {label}'.format(path=attr_name, label=label)
        exp_attr = {
            'name': ean,
            'value': eav
        }
        return exp, exp_attr, 'SET'

    def list_append(self, value, path=None, index=-1,
                    attr_label=None):
        path = attr_label or path or self.name
        label = ":{name}".format(name=path)
        attr_name = "#{name}".format(name=path)
        if index == 0:
            exp = "{path} = list_append({label}, {path})".format(
                path=attr_name, label=label)
        elif index == -1:
            exp = "{path} = list_append({path}, {label})".format(
                path=attr_name, label=label)
        else:
            raise ValidationException('index error')
        exp_attr = {
            'value': {label: value},
            'name': {attr_name: path}
        }
        return exp, exp_attr, 'SET'

    def remove(self, path=None, indexes=None):
        '''
        parameters:
            path: attr path
            index: (list) index ex: [2, 4]
        '''
        exp = ''
        path = path or self.name
        attr_name = "#{name}".format(name=path)
        ean = {attr_name: path}
        if self.field_type == 'list':
            for index in indexes:
                sub_exp = '{name}[{index}]'.format(name=attr_name,
                                                   index=index)
                if not exp:
                    exp = '{sub_exp}'.format(sub_exp=sub_exp)
                else:
                    exp = '{exp}, {sub_exp}'.format(exp=exp,
                                                    sub_exp=sub_exp)
        else:
            exp = '{path}'.format(path=path)
        return exp, {'name': ean}, 'REMOVE'

    def add(self, value, path=None, attr_label=None):
        '''
        support num and set
        ADD Price :n    price += n
        ADD Color :c
        '''
        if self.field_type not in ('integer', 'float', 'set', 'dict'):
            raise ValidationException('Incorrect data type, only [integer, float, set, dict]')
        exp_attr = {}
        if not path:
            attr_name = "#{name}".format(name=attr_label or self.name)
            exp_attr['name'] = {attr_name: self.name}
        else:
            attr_name = attr_label or path
        label = ":{name}".format(name=self.name)
        exp = '{name} {label}'.format(name=attr_name, label=label)
        exp_attr['value'] = {label: value}
        return exp, exp_attr, 'ADD'

    def typecast_for_storage(self, value):
        return smart_unicode(value)

    def _expression_func(self, op, *values, **kwargs):
        # for use by index ... bad
        values = map(self.typecast_for_storage, values)
        self.op = op
        self.express_args = values
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
        return self, func(*values), use_key

    def _expression(self, op, value):
        if self.use_decimal_types:
            value = Decimal(str(value))
        label = ':%s' % self.name
        exp = '{name} {op} {value}'.format(name=self.name, op=op, value=label)
        return exp, label, value

    def eq(self, value):  # ==
        # Creates a condition where the attribute is equal to the value.
        return self._expression_func('eq', value)

    def ne(self, value):  # !=
        # Creates a condition where the attribute is not equal to the value
        return self._expression_func('ne', value)

    def _eq(self, value):
        # Creates a condition where the attribute is equal to the value.
        return self._expression('=', value)

    def lt(self, value):  # <
        # Creates a condition where the attribute is less than the value.
        return self._expression_func('lt', value)

    def lte(self, value):  # <=
        # Creates a condition where the attribute is less than or
        # equal to the value.
        return self._expression_func('lte', value)

    def gt(self, value):  # >
        # Creates a condition where the attribute is greater than the value.
        return self._expression_func('gt', value)

    def gte(self, value):  # >=
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
