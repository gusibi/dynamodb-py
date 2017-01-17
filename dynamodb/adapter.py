#! -*- coding: utf-8 -*-

import pprint

from botocore.exceptions import ClientError
from botocore.vendored.requests.exceptions import ConnectionError

from .connection import db_local as db
from .helpers import get_attribute_type
# from dynamodb.connection import db

pp = pprint.PrettyPrinter(indent=4)
pprint = pp.pprint


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

    def _prepare_key_schema(self):
        KeySchema = []
        hash_key = self.instance._hash_key
        KeySchema.append({
            'AttributeName': hash_key,
            'KeyType': 'HASH'
        })
        range_key = self.instance._range_key
        if range_key:
            KeySchema.append({
                'AttributeName': range_key,
                'KeyType': 'RANGE'
            })
        return {'KeySchema': KeySchema}

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
        return {'AttributeDefinitions': AttributeDefinitions}

    def _prepare_primary_key(self, params):
        key_schema = self._prepare_key_schema()
        params.update(key_schema)
        attribute_definitions = self._prepare_attribute_definitions()
        params.update(attribute_definitions)
        return params

    def _prepare_indeics(self, table_params):
        return table_params

    def _prepare_create_table_params(self):
        # TableName
        table_params = {
            'TableName': self.table_name
        }
        # KeySchema && AttributeDefinitions
        table_params = self._prepare_primary_key(table_params)
        # ProvisionedThroughput
        table_params['ProvisionedThroughput'] = {
            'ReadCapacityUnits': self.instance.ReadCapacityUnits,
            'WriteCapacityUnits': self.instance.WriteCapacityUnits
        }
        # Index
        table_params = self._prepare_indeics(table_params)
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
        except ClientError:
            raise Exception('Cannot create preexisting table')
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

    def _update_global_indices(self):
        # TODO
        pass

    def _update_local_indices(self):
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
            print primary_key
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
        """
        try:
            response = self.table.query(**kwargs)
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])
        return response

    def scan(self, **kwargs):
        try:
            response = self.table.scan(**kwargs)
            print response
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])
        return response

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
            Expected={
                'string': {
                    'Value': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{},
                    'Exists': True|False,
                    'ComparisonOperator': 'EQ'|'NE'|'IN'|'LE'|'LT'|'GE'|'GT'|'BETWEEN'|'NOT_NULL'|'NULL'|'CONTAINS'|'NOT_CONTAINS'|'BEGINS_WITH',
                    'AttributeValueList': [
                        'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{},
                    ]
                }
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
