#! -*- coding: utf-8 -*-

from datetime import datetime

from dynamodb.models import Model
from dynamodb.fields import CharField, IntegerField, FloatField, DateTimeField
from dynamodb.connection import db_local as db


class Test(Model):

    __table_name__ = 'test'
    ReadCapacityUnits = 12
    WriteCapacityUnits = 12

    name = CharField(name='name', hash_key=True)
    score = IntegerField(name='score', range_key=True)
    order_score = FloatField(name='order_score')
    date = DateTimeField(name='date')

now = datetime.now()


def main():
    # Test.create_table()
    test = Test(name='gs', score=100, order_score=99.99, date=now)
    test.save()
    Test.create(name='gs1', score=100, order_score=99.99, date=now)
    Test.create(name='gs2', score=100, order_score=99.99, date=now)
    item = Test.get(name='gs', score=100)
    print item.name
    print Test.scan()
    # Test.delete_table()


if __name__ == '__main__':
    main()
