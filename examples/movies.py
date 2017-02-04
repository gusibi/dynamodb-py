#! -*- coding: utf-8 -*-

from __future__ import print_function  # Python 2/3 compatibility

from dynamodb.model import Model
from dynamodb.fields import (CharField, IntegerField, FloatField,
                             DateTimeField, DictField)


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
