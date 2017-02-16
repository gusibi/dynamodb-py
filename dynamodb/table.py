#! -*- coding: utf-8 -*-
from __future__ import print_function

import pprint

from botocore.exceptions import ClientError
from botocore.vendored.requests.exceptions import ConnectionError

from .connection import db_local as db
from .helpers import get_attribute_type
from .errors import UpdateItemException

pp = pprint.PrettyPrinter(indent=4)
pprint = pp.pprint

__all__ = ['Table']


class Table(object):

    def __init__(self, instance):
        self.instance = instance
        self.table_name = instance.__table_name__
        self.table = db.Table(self.table_name)

    def info(self):
        try:
            response = db.meta.client.describe_table(TableName=self.table_name)
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])
        else:
            table_info = response['Table']
        return table_info

    def _prepare_hash_key(self):
        hash_key = self.instance._hash_key
        param = {
            'AttributeName': hash_key,
            'KeyType': 'HASH'
        }
        return param

    def _prepare_range_key(self, range_key=None):
        if not range_key:
            range_key = self.instance._range_key
        if range_key:
            param = {
                'AttributeName': range_key,
                'KeyType': 'RANGE'
            }
            return param
        return {}

    def _prepare_key_schema(self):
        KeySchema = []
        hash_key_param = self._prepare_hash_key()
        KeySchema.append(hash_key_param)
        range_key_param = self._prepare_range_key()
        if range_key_param:
            KeySchema.append(range_key_param)
        return KeySchema

    def _prepare_attribute_definitions(self):
        AttributeDefinitions = []
        attributes = self.instance.attributes
        hash_key = self.instance._hash_key
        AttributeDefinitions.append({
            'AttributeName': hash_key,
            'AttributeType': get_attribute_type(attributes[hash_key]),
        })
        range_key = self.instance._range_key
        if range_key:
            AttributeDefinitions.append({
                'AttributeName': range_key,
                'AttributeType': get_attribute_type(attributes[range_key]),
            })
        for field in self.instance._local_indexed_fields:
            AttributeDefinitions.append({
                'AttributeName': field,
                'AttributeType': get_attribute_type(attributes[field]),
            })
        return AttributeDefinitions

    def _prepare_primary_key(self, params):
        params['KeySchema'] = self._prepare_key_schema()
        params['AttributeDefinitions'] = self._prepare_attribute_definitions()
        return params

    def _prepare_local_indexes(self):
        indexes = []
        for field in self.instance._local_indexed_fields:
            index_name = '{table_name}_ix_{field}'.format(
                table_name=self.table_name, field=field)
            KeySchema = [self._prepare_hash_key()]
            range_key_param = self._prepare_range_key(field)
            if range_key_param:
                KeySchema.append(range_key_param)
            indexes.append({
                'IndexName': index_name,
                'KeySchema': KeySchema,
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            })
        return indexes

    def _prepare_global_indexes(self):
        return []

    def _prepare_create_table_params(self):
        # TableName
        table_params = {
            'TableName': self.table_name
        }
        # KeySchema && AttributeDefinitions
        table_params = self._prepare_primary_key(table_params)
        # LocalSecondaryIndexes
        local_indexes = self._prepare_local_indexes()
        if local_indexes:
            table_params['LocalSecondaryIndexes'] = local_indexes
        # GlobalSecondaryIndexes
        global_indexes = self._prepare_global_indexes()
        if global_indexes:
            table_params['GlobalSecondaryIndexes'] = global_indexes
        # ProvisionedThroughput
        table_params['ProvisionedThroughput'] = {
            'ReadCapacityUnits': self.instance.ReadCapacityUnits,
            'WriteCapacityUnits': self.instance.WriteCapacityUnits
        }
        return table_params

    def create(self):
        '''
        # create table
        create_table Request Syntax
        # http://boto3.readthedocs.io/en/sinstance/reference/services/dynamodb.html#DynamoDB.Client.create_instance
        response = client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'string',
                    'AttributeType': 'S'|'N'|'B'
                },
            ],
            TableName='string',
            KeySchema=[
                {
                    'AttributeName': 'string',
                    'KeyType': 'HASH'|'RANGE'
                },
            ],
            LocalSecondaryIndexes=[
                {
                    'IndexName': 'string',
                    'KeySchema': [
                        {
                            'AttributeName': 'string',
                            'KeyType': 'HASH'|'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'|'KEYS_ONLY'|'INCLUDE',
                        'NonKeyAttributes': [
                            'string',
                        ]
                    }
                },
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'string',
                    'KeySchema': [
                        {
                            'AttributeName': 'string',
                            'KeyType': 'HASH'|'RANGE'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'|'KEYS_ONLY'|'INCLUDE',
                        'NonKeyAttributes': [
                            'string',
                        ]
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 123,
                        'WriteCapacityUnits': 123
                    }
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 123,
                'WriteCapacityUnits': 123
            },
            StreamSpecification={
                'StreamEnabled': True|False,
                'StreamViewType': 'NEW_IMAGE'|'OLD_IMAGE'|'NEW_AND_OLD_IMAGES'|'KEYS_ONLY'
            }
        )

        AttributeType (string) -- [REQUIRED]
            The data type for the attribute, where:

            * S - the attribute is of type String
            * N - the attribute is of type Number
            * B - the attribute is of type Binary
        KeySchema (list) -- [REQUIRED]
        KeyType - The role that the key attribute will assume:
            * HASH - partition key
            * RANGE - sort key
        '''
        try:
            params = self._prepare_create_table_params()
            return db.create_table(**params)
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])
        except ConnectionError:
            raise Exception('Connection refused')

    def _update_throughput(self, ProvisionedThroughput):
        ReadCapacityUnits = ProvisionedThroughput['ReadCapacityUnits']
        WriteCapacityUnits = ProvisionedThroughput['WriteCapacityUnits']
        if (ReadCapacityUnits != self.instance.ReadCapacityUnits or
                WriteCapacityUnits != self.instance.WriteCapacityUnits):
            self.table.update(ProvisionedThroughput={
                'ReadCapacityUnits': self.instance.ReadCapacityUnits,
                'WriteCapacityUnits': self.instance.WriteCapacityUnits
            })

    def _update_streams(self):
        # TODO
        pass

    def _update_global_indexes(self):
        # TODO
        pass

    def update(self):
        '''
        # update table
        http://boto3.readthedocs.io/en/stable/reference/services/dynamodb.html#DynamoDB.Table.update
        You can only perform one of the following operations at once:

        * Modify the provisioned throughput settings of the table.
        * Enable or disable Streams on the table.
        * Remove a global secondary index from the table.
        * Create a new global secondary index on the table.
          Once the index begins backfilling, you can use UpdateTable to perform
          other operations.

        UpdateTable is an asynchronous operation; while it is executing,
        the table status changes from ACTIVE to UPDATING. While it is UPDATING,
        you cannot issue another UpdateTable request.
        When the table returns to the ACTIVE state, the UpdateTable operation is
        complete.

        # Request Syntax

        {
           "AttributeDefinitions": [
              {
                 "AttributeName": "string",
                 "AttributeType": "string"
              }
           ],
           "GlobalSecondaryIndexUpdates": [
              {
                 "Create": {
                    "IndexName": "string",
                    "KeySchema": [
                       {
                          "AttributeName": "string",
                          "KeyType": "string"
                       }
                    ],
                    "Projection": {
                       "NonKeyAttributes": [ "string" ],
                       "ProjectionType": "string"
                    },
                    "ProvisionedThroughput": {
                       "ReadCapacityUnits": number,
                       "WriteCapacityUnits": number
                    }
                 },
                 "Delete": {
                    "IndexName": "string"
                 },
                 "Update": {
                    "IndexName": "string",
                    "ProvisionedThroughput": {
                       "ReadCapacityUnits": number,
                       "WriteCapacityUnits": number
                    }
                 }
              }
           ],
           "ProvisionedThroughput": {
              "ReadCapacityUnits": number,
              "WriteCapacityUnits": number
           },
           "StreamSpecification": {
              "StreamEnabled": boolean,
              "StreamViewType": "string"
           },
           "TableName": "string"
        }
        '''
        table_info = self.info()
        ProvisionedThroughput = table_info['ProvisionedThroughput']
        self._update_throughput(ProvisionedThroughput)

    def delete(self):
        # delete table
        try:
            return self.table.delete()
        except ClientError:
            raise Exception('Cannot do operations on a non-existent table')
        except ConnectionError:
            raise Exception('Connection refused')

    def _get_primary_key(self, **kwargs):
        hash_key, range_key = self.instance._hash_key, self.instance._range_key
        key = {
            hash_key: kwargs.get(hash_key) or getattr(self.instance, hash_key)
        }
        _range_key = kwargs.get(range_key) or getattr(self.instance, range_key, None)
        if range_key and not _range_key:
            raise Exception('Invalid range key value type')
        elif range_key:
            key[range_key] = _range_key
        return key

    def get_item(self, **kwargs):
        """
        primary_key: params: primary_key dict
        """
        kwargs['Key'] = kwargs.get('Key') or self._get_primary_key()
        try:
            response = self.table.get_item(**kwargs)
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])
        else:
            item = response.get('Item')
        return item

    def batch_get_item(self, *primary_keys):
        """
        primary_key: params: primary_keys list
        """
        _primary_keys = []
        for primary_key in primary_keys:
            key = self._get_primary_key(**primary_key)
            _primary_keys.append(key)
        params = {
            'RequestItems': {
                self.table_name: {
                    'Keys': _primary_keys
                }
            },
            'ReturnConsumedCapacity': 'TOTAL'
        }
        try:
            response = db.batch_get_item(**params)
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])
        else:
            items = response['Responses'][self.table_name]
        return items

    def put_item(self, item):
        self.table.put_item(Item=item)
        return True

    def batch_write(self, items, overwrite=False):
        pkeys = []
        if overwrite:
            instance = self.instance
            pkeys = [instance._hash_key, instance._range_key]
        try:
            with self.table.batch_writer(overwrite_by_pkeys=pkeys) as batch:
                for item in items:
                    batch.put_item(Item=item)
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])

    def query(self, **kwargs):
        """
        response = table.query(
            IndexName='string',
            Select='ALL_ATTRIBUTES'|'ALL_PROJECTED_ATTRIBUTES'|'SPECIFIC_ATTRIBUTES'|'COUNT',
            Limit=123,
            ConsistentRead=True|False,
            ScanIndexForward=True|False,
            ExclusiveStartKey={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            },
            ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
            ProjectionExpression='string',
            FilterExpression=Attr('myattribute').eq('myvalue'),
            KeyConditionExpression=Key('mykey').eq('myvalue'),
            ExpressionAttributeNames={
                'string': 'string'
            },
            ExpressionAttributeValues={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            }
        )
        ExclusiveStartKey: 起始查询的key，也就是上一页的最后一条数据
        ConsistentRead: 是否使用强制一致性 默认False
        ScanIndexForward: 索引的排序方式 True 为正序 False 为倒序 默认True
        ReturnConsumedCapacity: DynamoDB 写入期间使用的写入容量单位
            TOTAL 会返回由表及其所有global secondary index占用的写入容量；
            INDEXES 仅返回由global secondary index占用的写入容量；
            NONE 表示您不需要返回任何占用容量统计数据。
        ProjectionExpression: 用于指定要在扫描结果中包含的属性
        FilterExpression: 指定一个条件，以便仅返回符合条件的项目
        KeyConditionExpression: 要查询的键值
        ExpressionAttributeNames: 提供名称替换功能
        ExpressionAttributeValues: 提供值替换功能
        """
        try:
            response = self.table.query(**kwargs)
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])
        return response

    def scan(self, **kwargs):
        try:
            response = self.table.scan(**kwargs)
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])
        return response

    def _prepare_update_item_params(self, update_fields=None, *args, **kwargs):
        params = {
            'Key': self._get_primary_key()
        }
        ExpressionAttributeValues = getattr(self.instance,
                                            'ExpressionAttributeValues', {})
        ExpressionAttributeNames = getattr(self.instance,
                                           'ExpressionAttributeNames', {})
        action_exp_dict = {}
        if update_fields:
            set_expression_str = ''
            for k, v in update_fields.items():
                label = ':{k}'.format(k=k)
                path = '#{k}'.format(k=k)
                if set_expression_str:
                    set_expression_str += ', {k} = {v}'.format(k=path, v=label)
                else:
                    set_expression_str += '{k} = {v}'.format(k=path, v=label)
                ExpressionAttributeValues[label] = v
                ExpressionAttributeNames[path] = k
            action_exp_dict['SET'] = set_expression_str
        for arg in args:
            exp, exp_attr, action = arg
            eav = exp_attr.get('value', {})
            ean = exp_attr.get('name', {})
            action_exp = action_exp_dict.get(action)
            if action_exp:
                action_exp = '{action_exp}, {exp}'.format(action_exp=action_exp,
                                                          exp=exp)
            else:
                action_exp = exp
            action_exp_dict[action] = action_exp
            ExpressionAttributeValues.update(eav)
            ExpressionAttributeNames.update(ean)
        for action, _exp in action_exp_dict.iteritems():
            action_exp_dict[action] = '{action} {exp}'.format(action=action,
                                                              exp=_exp)
        if ExpressionAttributeValues:
            params['ExpressionAttributeValues'] = ExpressionAttributeValues
        if ExpressionAttributeNames:
            params['ExpressionAttributeNames'] = ExpressionAttributeNames
        params['UpdateExpression'] = " ".join(action_exp_dict.values())
        params.update(kwargs)
        return params

    def update_item(self, update_fields, *args, **kwargs):
        '''
        update_fields: update_fields (dict)
        http://boto3.readthedocs.io/en/stable/reference/services/dynamodb.html#DynamoDB.Table.update_item
        response = table.update_item(
            Key={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            },
            ReturnValues='NONE'|'ALL_OLD'|'UPDATED_OLD'|'ALL_NEW'|'UPDATED_NEW',
            ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
            ReturnItemCollectionMetrics='SIZE'|'NONE',
            UpdateExpression='string',
            ConditionExpression=Attr('myattribute').eq('myvalue'),
            ExpressionAttributeNames={
                'string': 'string'
            },
            ExpressionAttributeValues={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            }
        )
        ## example
        item.update_item(a=12, b=12, c=12)
        '''
        params = self._prepare_update_item_params(update_fields, *args, **kwargs)
        try:
            print(params)
            item = self.table.update_item(**params)
            attributes = item.get('Attributes')
            return attributes
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(e.response['Error']['Message'])
            raise UpdateItemException(e.response['Error']['Message'])

    def delete_item(self, **kwargs):
        '''
        http://boto3.readthedocs.io/en/stable/reference/services/dynamodb.html#DynamoDB.Table.delete_item
        Deletes a single item in a table by primary key. You can perform a
        conditional delete operation that deletes the item if it exists,
        or if it has an expected attribute value.

        In addition to deleting an item, you can also return the item's
        attribute values in the same operation, using the ReturnValues parameter.

        Unless you specify conditions, the DeleteItem is an idempotent operation;
        running it multiple times on the same item or attribute does not result
        in an error response.

        Conditional deletes are useful for deleting items only if specific
        conditions are met. If those conditions are met, DynamoDB performs the
        delete. Otherwise, the item is not deleted.

        Request Syntax

        response = table.delete_item(
            Key={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            },
            ConditionalOperator='AND'|'OR',
            ReturnValues='NONE'|'ALL_OLD'|'UPDATED_OLD'|'ALL_NEW'|'UPDATED_NEW',
            ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
            ReturnItemCollectionMetrics='SIZE'|'NONE',
            ConditionExpression=Attr('myattribute').eq('myvalue'),
            ExpressionAttributeNames={
                'string': 'string'
            },
            ExpressionAttributeValues={
                'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
            }
        )
        Parameters:
            Key (dict) -- [REQUIRED]
        '''
        key = self._get_primary_key()
        try:
            self.table.delete_item(Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                raise Exception(e.response['Error']['Message'])
        return True

    def item_count(self):
        return self.table.item_count
