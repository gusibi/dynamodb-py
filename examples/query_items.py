#! -*- coding: utf-8 -*-
from __future__ import print_function  # Python 2/3 compatibility

import boto3
import json
import decimal
from botocore.exceptions import ClientError

from movies import Movies


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


dynamodb = boto3.resource("dynamodb", region_name='us-west-2',
                          endpoint_url="http://localhost:8000")

title = "Die Hard 2"
year = 1990


def query_by_boto3():
    table = dynamodb.Table('Movies')
    try:
        response = table.query(
            Key={
                'year': year,
                'title': title
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item = response['Item']
        print("GetItem succeeded:")
        print(json.dumps(item, indent=4, cls=DecimalEncoder))


def query_without_index():
    item = Movies.get(year=year, title=title)
    print("GetItem succeeded:", item.info, item.year)

    primary_keys = [
        {'year': 1990, 'title': 'Edward Scissorhands'},
        {'year': 1990, 'title': 'Ghost'},
        {'year': 2007, 'title': 'Captivity'},
    ]
    items = Movies.batch_get(*primary_keys)
    print('items len:', len(items))


def query_with_index():
    item = Movies.get(year=year, title=title)
    print("GetItem succeeded:", item.info, item.year)

    primary_keys = [
        {'year': 1990, 'title': 'Edward Scissorhands'},
        {'year': 1990, 'title': 'Ghost'},
        {'year': 2007, 'title': 'Captivity'},
    ]
    items = Movies.batch_get(*primary_keys)
    print('items len:', len(items))


if __name__ == '__main__':
    query_by_boto3()
    query_with_index()
    query_without_index()
