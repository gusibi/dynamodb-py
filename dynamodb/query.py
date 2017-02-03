#! -*- coding: utf-8 -*-

import copy

from .adapter import Table
from .fields import Fields
from .errors import FieldValidationError


class Query(object):

    def __init__(self, model_object, *args, **kwargs):
        self.Scan = False
        self.model_object = model_object
        self.model_class = self.model_object.__class__
        self.instance = self.model_class()
        self.ProjectionExpression = self._projection_expression(*args)
        self.ReturnConsumedCapacity = 'TOTAL'  # 'INDEXES'|'TOTAL'|'NONE'
        self.ConsistentRead = False
        self.FilterExpression = None
        self.ExclusiveStartKey = None  # 起始查询的key，就是上一页的最后一条数据
        self.KeyConditionExpression = None
        self.ExpressionAttributeNames = {}
        self.ExpressionAttributeValues = {}
        self.ScanIndexForward = False    # True|False
        self.ConditionalOperator = None  # 'AND'|'OR'
        self.IndexName = None
        self.Select = 'ALL_ATTRIBUTES'  # 'ALL_ATTRIBUTES'|'ALL_PROJECTED_ATTRIBUTES'|'SPECIFIC_ATTRIBUTES'|'COUNT'
        self.Limit = None
        self.scaned_count = 0
        self.count = 0
        self.query_params = {}
        self.filter_args = []   # filter expression args
        self.filter_index_field = None  # index field name

    @property
    def consistent(self):
        self.ConsistentRead = True
        return copy.deepcopy(self)

    @property
    def scan(self):
        self.Scan = True
        return copy.deepcopy(self)

    def _projection_expression(self, *args):
        instance = self.model_object
        projections = []
        for arg in args:
            if isinstance(arg, Fields):
                name = arg.name
                if arg not in instance.fields:
                    raise FieldValidationError('%s not found' % name)
                projections.append(name)
            else:
                raise FieldValidationError('Bad type must be Attribute type')
        ProjectionExpression = ",".join(projections)
        return ProjectionExpression

    def _get_primary_key(self):
        hash_key, range_key = self.instance._hash_key, self.instance._range_key
        key = {
            hash_key: getattr(self.instance, hash_key)
        }
        _range_key = getattr(self.instance, range_key, None)
        if range_key and not _range_key:
            raise Exception('Invalid range key value type')
        elif range_key:
            key[range_key] = _range_key
        return key

    def _filter_expression(self, *args):
        # get filter expression and key condition expression
        FilterExpression = None
        KeyConditionExpression = None
        params = {}
        for field_inst, exp, is_key in args:
            # field_inst = field_instance
            if field_inst.name == self.filter_index_field:
                _, exp, is_key = field_inst._expression_func(
                    field_inst.op, *field_inst.express_args, use_key=True)
            if is_key:
                if not KeyConditionExpression:
                    KeyConditionExpression = exp
                else:
                    KeyConditionExpression = KeyConditionExpression & exp
            else:
                if not FilterExpression:
                    FilterExpression = exp
                else:
                    FilterExpression = FilterExpression & exp
        if FilterExpression:
            params['FilterExpression'] = FilterExpression
        if KeyConditionExpression:
            params['KeyConditionExpression'] = KeyConditionExpression
        return params

    def _get_query_params(self):
        # get filter params
        params = {}
        # update filter expression
        filter_params = self._filter_expression(*self.filter_args)
        FilterExpression = filter_params.get('FilterExpression')
        if FilterExpression:
            params['FilterExpression'] = FilterExpression
        KeyConditionExpression = filter_params.get('KeyConditionExpression')
        if KeyConditionExpression:
            params['KeyConditionExpression'] = KeyConditionExpression
        if self.ProjectionExpression:
            params['ProjectionExpression'] = self.ProjectionExpression
        if self.ConsistentRead:
            params['ConsistentRead'] = self.ConsistentRead
        if self.ReturnConsumedCapacity:
            params['ReturnConsumedCapacity'] = self.ReturnConsumedCapacity
        self.query_params.update(params)
        return self.query_params

    def where(self, *args):
        # Find by any number of matching criteria... though presently only
        # "where" is supported.
        self.filter_args = args
        return copy.deepcopy(self)

    def limit(self, limit):
        self.Limit = limit
        self.query_params['Limit'] = limit
        return copy.deepcopy(self)

    def _get_item_params(self):
        params = {
            'Key': self._get_primary_key()
        }
        if self.ProjectionExpression:
            params['ProjectionExpression'] = self.ProjectionExpression
        if self.ConsistentRead:
            params['ConsistentRead'] = self.ConsistentRead
        return params

    def get(self, **primary_key):
        # get directly by primary key
        self.instance = self.model_class(**primary_key)
        params = self._get_item_params()
        item = Table(self.instance).get_item(**params)
        if not item:
            return None
        value_for_read = self.instance._get_values_for_read(item)
        return value_for_read

    def first(self):
        items = self.limit(1).all()
        return items[0] if items else None

    def order_by(self, index_field, asc=True):
        if isinstance(index_field, Fields):
            name = index_field.name
            index_name = self.instance._local_indexes.get(name)
            if not index_name:
                raise Exception('index not found')
            self.filter_index_field = name
            self.query_params.update({
                'IndexName': index_name,
                'ScanIndexForward': asc
            })
        else:
            raise Exception('%s not a field')
        return copy.deepcopy(self)

    def _yield_all(self, method):
        if method == 'scan':
            func = getattr(Table(self.instance), 'scan')
        elif method == 'query':
            func = getattr(Table(self.instance), 'query')
        else:
            return
        result_count = 0
        response = func(**self.query_params)
        while True:
            metadata = response.get('ResponseMetadata', {})
            for item in response['Items']:
                result_count += 1
                yield item
                if self.Limit > 0 and result_count >= self.Limit:
                    return
            LastEvaluatedKey = response.get('LastEvaluatedKey')
            if LastEvaluatedKey:
                self.query_params['ExclusiveStartKey'] = LastEvaluatedKey
                response = func(**self.query_params)
            else:
                break

    def _yield(self):
        if self.Scan:
            return self._yield_all('scan')
        else:
            return self._yield_all('query')

    def all(self):
        self._get_query_params()
        if self.Scan:
            func = getattr(Table(self.instance), 'scan')
            return self._yield_all('scan')
        else:
            func = getattr(Table(self.instance), 'query')
        response = func(**self.query_params)
        items = response['Items']
        results = []
        for item in items:
            _instance = self.model_class(**item)
            value_for_read = _instance._get_values_for_read(item)
            instance = self.model_class(**value_for_read)
            results.append(instance)
        return results
