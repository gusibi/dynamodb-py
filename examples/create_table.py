#! -*- coding: utf-8 -*-

from __future__ import print_function  # Python 2/3 compatibility
import boto3

from dynamodb.models import Model
from dynamodb.fields import CharField, IntegerField, FloatField, DictField
from dynamodb.adapter import Table

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


def create_table():
    # Table(Movies()).delete()
    table = Table(Movies()).create()
    print("Table status:", table.table_status)
    print("Table info:", Table(Movies()).info())
    print("Table indexes", Movies()._local_indexes)


if __name__ == '__main__':
    create_table()
