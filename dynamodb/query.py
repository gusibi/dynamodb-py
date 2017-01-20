#! -*- coding: utf-8 -*-

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
        self.ReturnConsumedCapacity = 'NONE'  # 'INDEXES'|'TOTAL'|'NONE'
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
        self.query_params = {}
        self.scaned_count = 0
        self.count = 0

    @property
    def consistent(self):
        self.ConsistentRead = True
        return self

    @property
    def scan(self):
        self.Scan = True
        return self

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

    def _filter_expression(self, *args):
        # get filter expression and key condition expression
        FilterExpression = None
        KeyConditionExpression = None
        params = {}
        for exp, is_key in args:
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
            self.FilterExpression = FilterExpression
        if KeyConditionExpression:
            params['KeyConditionExpression'] = KeyConditionExpression
            self.KeyConditionExpression = KeyConditionExpression
        return params

    def _get_filter_params(self):
        # get filter params
        params = {}
        if self.ProjectionExpression:
            params['ProjectionExpression'] = self.ProjectionExpression
        if self.ConsistentRead:
            params['ConsistentRead'] = self.ConsistentRead
        if self.FilterExpression:
            params['FilterExpression'] = self.FilterExpression
        if self.KeyConditionExpression:
            params['KeyConditionExpression'] = self.KeyConditionExpression
        return params

    def where(self, *args):
        # Find by any number of matching criteria... though presently only
        # "where" is supported.
        self._filter_expression(*args)
        self.query_params = self._get_filter_params()
        return self

    def order_by(self, *args):
        return self

    def limit(self, limit):
        self.Limit = limit
        self.query_params['Limit'] = limit
        return self

    def first(self):
        self.query_params['Limit'] = 1
        items = self.all()
        return items[0] if items else None

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
        if self.Scan:
            func = getattr(Table(self.instance), 'scan')
            return self._yield_all('scan')
        else:
            func = getattr(Table(self.instance), 'query')
        response = func(**self.query_params)
        return response['Items']
