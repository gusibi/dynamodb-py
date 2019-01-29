#! -*- coding: utf-8 -*-
from __future__ import print_function  # Python 2/3 compatibility
from os import environ

environ['DEBUG'] = '1'

import boto3
import json
import decimal
from botocore.exceptions import ClientError

from movies import Movies, Authentication


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


def get_item_by_boto3():
    table = dynamodb.Table('Movies')
    try:
        response = table.get_item(
            Key={
                'year': year,
                'title': title
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print(response)
        item = response.get('Item')
        if item:
            print("GetItem succeeded:")
            print(json.dumps(item, indent=4, cls=DecimalEncoder))
        else:
            print("NotFound")


def get_item():
    item = Movies.get(year=year, title=title)
    if item:
        print("GetItem succeeded:", item.info, item.year)
    else:
        print("GetItem not found")

    primary_keys = [
        {'year': 1990, 'title': 'Edward Scissorhands'},
        {'year': 1990, 'title': 'Ghost'},
        {'year': 2007, 'title': 'Captivity'},
    ]
    items = Movies.batch_get(*primary_keys)
    print('items len:', len(items))


def get_item_with_global_index():
    Authentication.create(identity='123', approach='mobile', account_id="123")
    Authentication.create(identity='124', approach='weixin', account_id="123")
    Authentication.create(identity='125', approach='mobile', account_id="125")
    Authentication.create(identity='126', approach='weixin', account_id="126")
    item = (Authentication.global_query(index_name='authentication_account_id-index')
            .get(account_id="123", approach='mobile'))
    print(item)
    print("Authentication with account_id and approach")
    print(item.account_id, ":", item.approach, item.identity)


if __name__ == '__main__':
    get_item_by_boto3()
    get_item()
    get_item_with_global_index()
