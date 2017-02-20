#! -*- coding: utf-8 -*-
from __future__ import print_function  # Python 2/3 compatibility
from os import environ

import boto3
import decimal
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

from dynamodb.json_import import json

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
    print("Movies from %s" % year)
    try:
        response = table.query(
            KeyConditionExpression=Key('year').eq(year),
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        items = response['Items']
        for i in items:
            print(i['year'], ":", i['title'])


def query_with_limit_by_boto3():
    table = dynamodb.Table('Movies')
    print("Movies from %s" % year)
    try:
        response = table.query(
            KeyConditionExpression=Key('year').eq(1985),
            Limit=10,
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        items = response['Items']
        for i in items:
            print(i['year'], ":", i['title'])


def query_with_filter_by_boto3():
    table = dynamodb.Table('Movies')
    print("Movies from 1992 - titles A-L, with genres and lead actor")
    try:
        response = table.query(
            ProjectionExpression="#yr, title, info.genres, info.actors[0]",
            ExpressionAttributeNames={"#yr": "year"}, # Expression Attribute Names for Projection Expression only.
            KeyConditionExpression=Key('year').eq(1992) & Key('title').between('A', 'L'),
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        items = response['Items']
        for i in items:
            print(i['year'], ":", i['title'])


def query_with_limit_and_filter_by_boto3():
    table = dynamodb.Table('Movies')
    print("Movies from 1992 - titles A-L, with genres and lead actor")
    try:
        response = table.query(
            ProjectionExpression="#yr, title, info.genres, info.actors[0]",
            ExpressionAttributeNames={"#yr": "year"}, # Expression Attribute Names for Projection Expression only.
            KeyConditionExpression=Key('year').eq(1992) & Key('title').between('A', 'L'),
            FilterExpression=Attr('rating').lt(decimal.Decimal(str('7.0'))),
            Limit=10,
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        items = response['Items']
        for i in items:
            print(i['year'], ":", i['title'])


def query_with_paginator_by_boto3():
    client = dynamodb.meta.client
    paginator = client.get_paginator('query')
    print("Movies from 1992 - titles A-L, with genres and lead actor")
    try:
        response = paginator.paginate(
            TableName='Movies',
            ProjectionExpression="#yr, title, info.genres, info.actors[0]",
            ExpressionAttributeNames={"#yr": "year"}, # Expression Attribute Names for Projection Expression only.
            KeyConditionExpression=Key('year').eq(1992) & Key('title').between('A', 'Z'),
            Limit=20,
            PaginationConfig={
                'MaxItems': 20,
                'PageSize': 20,
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        for _items in response:
            items = _items['Items']
            print(items)
            y = _items['LastEvaluatedKey']['year']
            print(y, type(y))
            for i in items:
                print(i['year'], type(i['year']), ":", i['title'])


def query_without_index():
    # query_by_boto3()
    response = Movies.query().where(Movies.year.eq(year)).all()
    items = response['Items']
    print("Movies from %s" % year)
    for i in items:
        print(i.year, ":", i.title)

    # query_with_limit_by_boto3()
    response = Movies.query().where(Movies.year.eq(1985)).limit(10).all()
    items = response['Items']
    print("Movies from %s limit 10" % year)
    for i in items:
        print(i.year, ":", i.title)

    # query_with_filter_by_boto3()
    response = (Movies.query()
                .where(Movies.year.eq(1992),
                       Movies.title.between('A', 'L'))
                .all())
    items = response['Items']
    print("Movies from 1992 - titles A-L, with genres and lead actor")
    for i in items:
        print(i.year, ":", i.title)

    # query_with_limit_and_filter_by_boto3()
    response = (Movies.query()
                .where(Movies.year.eq(1992),
                       Movies.title.between('A', 'L'),
                       Movies.rating.eq('7.0'))
                .all())
    items = response['Items']
    print("Movies from 1992 - titles A-L, with genres and lead actor")
    for i in items:
        print(i.year, ":", i.title)


def query_with_index():
    response = (Movies.query()
                .where(Movies.year.eq(1992),
                       Movies.rating.between(6.0, 7.9))
                .order_by(Movies.rating)  # use rating as range key by local index
                .all())
    items = response['Items']
    print("Movies from 1992 - rating 6.0-7.9, with genres and lead actor")
    for i in items:
        print(i.year, ":", i.title, i.rating)


def query_with_paginator():
    response = (Movies.query()
                .where(Movies.year.eq(1992),
                       Movies.title.between('A', 'L'))
                .limit(20)
                .all())
    items = response['Items']
    print(response)
    LastEvaluatedKey = response['LastEvaluatedKey']
    print(LastEvaluatedKey)
    for i in items:
        print(i.year, ":", i.title)

    response = (Movies.query()
                .start_key(**LastEvaluatedKey)
                .where(Movies.year.eq(1992),
                       Movies.title.between('A', 'L'))
                .limit(20)
                .all())
    items = response['Items']
    print(response)
    LastEvaluatedKey = response['LastEvaluatedKey']
    print(LastEvaluatedKey)
    for i in items:
        print(i.year, ":", i.title)

if __name__ == '__main__':
    # query_by_boto3()
    # query_with_limit_by_boto3()
    # query_with_filter_by_boto3()
    # query_with_limit_and_filter_by_boto3()
    # query_without_index()
    # query_with_index()
    # query_with_paginator_by_boto3()
    query_with_paginator()
