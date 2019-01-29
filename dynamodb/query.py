#! -*- coding: utf-8 -*-
from __future__ import print_function

import copy

from .table import Table
from .fields import Fields
from .connection import db
from .errors import FieldValidationException, GlobalSecondaryIndexesException


class Paginator(object):

    def __init__(self, model_object):
        self.model_object = model_object

    def query(self, **kwargs):
        '''
        response_iterator = paginator.paginate(
            TableName='string',
            IndexName='string',
            Select='ALL_ATTRIBUTES'|'ALL_PROJECTED_ATTRIBUTES'|'SPECIFIC_ATTRIBUTES'|'COUNT',
            ConsistentRead=True|False,
            ScanIndexForward=True|False,
            ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
            ProjectionExpression='string',
            FilterExpression='string',
            KeyConditionExpression='string',
            ExpressionAttributeNames={
                'string': 'string'
            },
            ExpressionAttributeValues={
                'string': {
                    'S': 'string',
                    'N': 'string',
                    'B': b'bytes',
                    'SS': [
                        'string',
                    ],
                    'NS': [
                        'string',
                    ],
                    'BS': [
                        b'bytes',
                    ],
                    'M': {
                        'string': {'... recursive ...'}
                    },
                    'L': [
                        {'... recursive ...'},
                    ],
                    'NULL': True|False,
                    'BOOL': True|False
                }
            },
            PaginationConfig={
                'MaxItems': 123,
                'PageSize': 123,
                'StartingToken': 'string'
            }
        )
        '''
        client = db.meta.client
        paginator = client.get_paginator('query')
        limit = kwargs.pop('Limit', None)
        kwargs['TableName'] = self.model_object.__table_name__
        if limit is not None:
            pagination_config = {
                'MaxItems': limit,
                'PageSize': limit,
                'StaringToken': '123232'
            }
            kwargs['PaginationConfig'] = pagination_config
        # response = paginator.paginate(**kwargs).build_full_result()
        response = paginator.paginate(**kwargs)
        for item in response:
            pass
        return item


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
        self.Offset = None
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

    def start_key(self, **kwargs):
        self.ExclusiveStartKey = kwargs
        self.query_params['ExclusiveStartKey'] = self.ExclusiveStartKey
        return copy.deepcopy(self)

    def _projection_expression(self, *args):
        instance = self.model_object
        projections = []
        for arg in args:
            
            if isinstance(arg, Fields):
                name = arg.name
                if arg not in instance.fields:
                    raise FieldValidationException('%s not found' % name)
                projections.append(name)
            else:
                raise FieldValidationException('Bad type must be Attribute type')
        ProjectionExpression = ",".join(projections)
        return ProjectionExpression

    def _get_primary_key(self):
        hash_key, range_key = self.instance._hash_key, self.instance._range_key
        key = {
            hash_key: getattr(self.instance, hash_key)
        }
        _range_key = getattr(self.instance, range_key, None)
        if range_key and not _range_key:
            raise FieldValidationException('Invalid range key value type')
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
                    field_inst.op, *field_inst.express_args, is_key=True)
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
        self.filter_args.extend(args)
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
        response = self.limit(1).all()
        items = response['Items']
        return items[0] if items else None

    def order_by(self, index_field, asc=True):
        if isinstance(index_field, Fields):
            name = index_field.name
            index_name = self.instance._local_indexes.get(name)
            if not (index_name or index_field.range_key):
                raise FieldValidationException('index not found')
            self.filter_index_field = name
            if index_name:
                self.query_params['IndexName'] = index_name
            self.query_params['ScanIndexForward'] = asc
        else:
            raise FieldValidationException('%s not a field' % index_field)
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
        response['Items'] = results
        return response


class GlobalQuery(Query):

    def __init__(self, model_object, index_name, *args, **kwargs):
        super(GlobalQuery, self).__init__(model_object, *args, **kwargs)
        self.GlobalSecondaryIndexes = {g['name']: g for g in self.instance._global_secondary_indexes}
        if index_name not in self.GlobalSecondaryIndexes:
            raise  GlobalSecondaryIndexesException("Invalid GSI")
        self.GSI = index_name
        self.GSIHashKey = None
        self.GSIRangeKey = None
        self.init_gsi()

    def init_gsi(self):
        gindex = self.GlobalSecondaryIndexes[self.GSI]
        hash_key, range_key = gindex['hash_key'], gindex.get('range_key', None)
        self.GSIHashKey = self.instance.attributes[hash_key]
        if range_key:
            self.GSIRangeKey = self.instance.attributes[range_key]

    def _projection_expression(self, *args):
        instance = self.model_object
        projections = []
        for arg in args:
            if isinstance(arg, Fields):
                name = arg.name
                if arg not in instance.fields:
                    raise FieldValidationException('%s not found' % name)
                projections.append(name)
            else:
                raise FieldValidationException('Bad type must be Attribute type')
        ProjectionExpression = ",".join(projections)
        return ProjectionExpression

    def _get_primary_key(self):
        hash_key, range_key = self.GSIHashKey, self.GSIRangeKey
        key = {
            "hash_key": hash_key
        }
        if range_key:
            key["range_key"] = range_key
        return key

    def _filter_expression(self, *args):
        # get filter expression and key condition expression
        FilterExpression = None
        KeyConditionExpression = None
        params = {}
        for field_inst, exp, is_key in args:
            # field_inst = field_instance
            if (field_inst.name == self.GSIHashKey.name or 
                (self.GSIRangeKey and field_inst.name == self.GSIRangeKey.name)):
                _, exp, is_key = field_inst._expression_func(
                    field_inst.op, *field_inst.express_args, is_key=True)
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
        params = {"IndexName": self.GSI}
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
        self.filter_args.extend(args)
        return copy.deepcopy(self)

    def limit(self, limit):
        self.Limit = limit
        self.query_params['Limit'] = limit
        return copy.deepcopy(self)

    def get(self, **primary_key):
        # get directly by primary key
        hash_key_value, range_key_value = None, None
        for k, v in primary_key.items():
            print(k, v)
            if k == self.GSIHashKey.name:
                hash_key_value = v
            elif k == self.GSIRangeKey.name:
                range_key_value = v
            else:
                raise GlobalSecondaryIndexesException('Invalid primary key')
        self.instance = self.model_class(**primary_key)
        if not hash_key_value:
            raise GlobalSecondaryIndexesException('Invalid primary key')
        if self.GSIRangeKey and not range_key_value:
            raise GlobalSecondaryIndexesException('Invalid primary key')
        query = self.where(self.GSIHashKey.eq(hash_key_value))
        if range_key_value:
            query = query.where(self.GSIRangeKey.eq(range_key_value))
        item = query.first()
        return item

    def first(self):
        response = self.limit(1).all()
        items = response['Items']
        return items[0] if items else None

    def order_by(self, index_field, asc=True):
        if isinstance(index_field, Fields):
            name = index_field.name
            index_name = self.instance._local_indexes.get(name)
            if not (index_name or index_field.range_key):
                raise FieldValidationException('index not found')
            self.filter_index_field = name
            self.query_params['ScanIndexForward'] = asc
        else:
            raise FieldValidationException('%s not a field' % index_field)
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
        response['Items'] = results
        return response
