#! -*- coding: utf-8 -*-
from __future__ import print_function  # Python 2/3 compatibility
from os import environ

import boto3

environ['DEBUG'] = '1'

from dynamodb.model import Model
from dynamodb.fields import (CharField, IntegerField, FloatField,
                             DictField, BooleanField, DateTimeField)
from dynamodb.table import Table

'''
创建一个名为 Movies 的表。表的主键由以下属性组成：

year - 分区键。AttributeType 为 N，表示数字。
title - 排序键。AttributeType 为 S，表示字符串。

'''


dynamodb = boto3.resource('dynamodb', region_name='us-west-2',
                          endpoint_url="http://localhost:8000")


def create_table_by_boto3():
    table = dynamodb.create_table(
        TableName='Movies',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    print("Table status:", table.table_status)


class Movies(Model):

    __table_name__ = 'Movies'

    ReadCapacityUnits = 10
    WriteCapacityUnits = 10

    year = IntegerField(name='year', hash_key=True)
    title = CharField(name='title', range_key=True)
    rating = FloatField(name='rating', indexed=True)
    rank = IntegerField(name='rank', indexed=True)
    release_date = CharField(name='release_date')
    info = DictField(name='info', default={})


class Authentication(Model):
    __table_name__ = 'authentication'

    ReadCapacityUnits = 100
    WriteCapacityUnits = 10

    APPROACH_MOBILE = 'mobile'
    APPROACH_WEIXIN = 'weixin'
    APPROACH_MOBILE_PASSWORD = 'mobile_password'

    __global_indexes__ = [
        ('authentication_account_id-index', ('account_id', 'approach'), ['account_id', 'approach', 'identity', 'is_verified']),
    ]

    account_id = CharField(name='account_id')
    approach = CharField(name='approach', range_key=True)
    identity = CharField(name='identity', hash_key=True)
    is_verified = BooleanField(name='is_verified', default=False)
    date_created = DateTimeField(name='date_created')


def create_table():
    Table(Movies()).delete()
    table = Table(Movies()).create()
    print("Table status:", table.table_status)
    print("Table info:", Table(Movies()).info())
    print("Table indexes", Movies()._local_indexes)

    Table(Authentication()).delete()
    table = Table(Authentication()).create()
    print("Table status:", table.table_status)
    print("Table info:", Table(Authentication()).info())
    print("Table indexes", Authentication()._local_indexes)


if __name__ == '__main__':
    create_table()
