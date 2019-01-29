#! -*- coding: utf-8 -*-

from __future__ import print_function  # Python 2/3 compatibility

from dynamodb.model import Model
from dynamodb.fields import (CharField, IntegerField, FloatField,
                             DateTimeField, DictField, BooleanField)


class Movies(Model):

    __table_name__ = 'Movies'

    ReadCapacityUnits = 10
    WriteCapacityUnits = 10

    year = IntegerField(name='year', hash_key=True)
    title = CharField(name='title', range_key=True)
    rating = FloatField(name='rating', indexed=True)
    rank = IntegerField(name='rank', indexed=True)
    release_date = DateTimeField(name='release_date')
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