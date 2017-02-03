#! -*- coding: utf-8 -*-

from __future__ import print_function  # Python 2/3 compatibility
import boto3

from dynamodb.models import Model
from dynamodb.fields import (CharField, IntegerField, FloatField, DictField)
from dynamodb.adapter import Table

'''
创建一个名为 Movies 的表。表的主键由以下属性组成：

year - 分区键。AttributeType 为 N，表示数字。
title - 排序键。AttributeType 为 S，表示字符串。

'''

dynamodb = boto3.resource('dynamodb', region_name='us-west-2',
                          endpoint_url="http://localhost:8000")


def update_table_by_boto3():
    table = dynamodb.Table('Movies').update(
        TableName='Movies',
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
            'ReadCapacityUnits': 21,
            'WriteCapacityUnits': 21
        }
    )

    print("Table status:", table.table_status)


class Movies(Model):

    __table_name__ = 'Movies'

    ReadCapacityUnits = 20
    WriteCapacityUnits = 20

    year = IntegerField(name='year', hash_key=True)
    title = CharField(name='title', range_key=True)
    rating = FloatField(name='rating', indexed=True)
    rank = IntegerField(name='rank', indexed=True)
    release_date = CharField(name='release_date')
    info = DictField(name='info', default={})


def update_table():
    # Table(Movies()).delete()
    Table(Movies()).update()
    print("Table info:", Table(Movies()).info())
    print("Table indexes", Movies()._local_indexes)


if __name__ == '__main__':
    update_table_by_boto3()
    update_table()
