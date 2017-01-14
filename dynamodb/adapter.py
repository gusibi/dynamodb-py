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

    def _prepare_primary_key(self, params):
        KeySchema, AttributeDefinitions = [], []
        attributes = self.instance.attributes
        hash_key = self.instance._hash_key
        if not hash_key:
            raise Exception('Invalid hash_key')
        KeySchema.append({
            'AttributeName': hash_key,
            'KeyType': 'HASH'
        })
        AttributeDefinitions.append({
            'AttributeName': hash_key,
            'AttributeType': get_attribute_type(attributes[hash_key]),
        })
        range_key = self.instance._range_key
        if range_key:
            KeySchema.append({
                'AttributeName': range_key,
                'KeyType': 'RANGE'
            })
            AttributeDefinitions.append({
                'AttributeName': range_key,
                'AttributeType': get_attribute_type(attributes[range_key]),
            })
        params['KeySchema'] = KeySchema
        params['AttributeDefinitions'] = AttributeDefinitions
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
        # pprint(table_params)
        return table_params

    def create_table(self):
        '''
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

    def delete_table(self):
        try:
            return self.table.delete()
        except ClientError:
            raise Exception('Cannot do operations on a non-existent table')
        except ConnectionError:
            raise Exception('Connection refused')

    def get(self, **primary_keys):
        hash_key, range_key = self.instance._hash_key, self.instance._range_key
        key = {
            hash_key: primary_keys[hash_key]
        }
        if range_key:
            key[range_key] = primary_keys[range_key]
        try:
            response = self.table.get_item(Key=key)
        except ClientError as e:
            raise Exception(e.response['Error']['Message'])
        else:
            item = response['Item']
        return item

    def batch_get(self, **primary_keys):
        pass

    def write(self, item):
        self.table.put_item(Item=item)
        return True

    def batch_write(self):
        pass

    def scan(self):
        return self.table.scan()

    def delete(self, primary_keys):
        pass
