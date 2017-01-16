#! -*- coding: utf-8 -*-

from datetime import datetime

import decimal

from dynamodb.models import Model
from dynamodb.fields import (CharField, IntegerField, FloatField, Attribute,
                             DateTimeField, DictField, ListField)
from dynamodb.adapter import Table


class Test(Model):

    __table_name__ = 'test'
    ReadCapacityUnits = 1
    WriteCapacityUnits = 1

    name = CharField(name='name', hash_key=True)
    score = IntegerField(name='score', range_key=True)
    order_score = FloatField(name='order_score')
    category = CharField(name='category', default=' ')
    date = DateTimeField(name='date')
    ids = ListField(name='ids', default=[])
    doc = DictField(name='doc', default={})

now = datetime.now()


def show_table():
    table_info = Table(Test()).info()
    print table_info
    return table_info


def create_table():
    Table(Test()).create()


def update_table():
    Table(Test()).update()


def delete_table():
    Table(Test()).delete()


def save_item():
    test = Test(name='gs', score=100, order_score=99.99, date=now)
    test.save()


def create_and_get_item():
    Test.create(name='gs1', score=100, order_score=99.99, date=now)
    item1 = Test.get(name='gs1', score=100)
    print item1.name, item1.ids, item1.doc, item1.category
    # Test.create(name='gs2', score=100, order_score=99.99, date=now)
    # Test.create(name='gs4', score=100, order_score=99.99, date=now, ids=[1, 2,3])
    # Test.create(name='gs5', score=100, order_score=99.99, date=now, doc={'a': 1.1})


def delete_item():
    Test.create(name='gs6', score=100, order_score=99.99, date=now, ids=[1,2,4, 101], doc={'a': 1.1})
    item6 = Test.get(name='gs6', score=100)
    print item6.name, item6.ids, item6.doc
    print 'delete: >>>>>>>>>>>>>'
    item6.delete()
    print 'DeleteItem succeeded: >>>>>>>>>>>'
    item6 = Test.get(name='gs6', score=100)
    if item6:
        print item6.name, item6.ids, item6.doc
    else:
        print 'item6 not found'


def batch_add_and_get_item():
    items = [
        dict(name='gs7', score=90, order_score=90, date=now),
        dict(name='gs7', score=90, order_score=90, date=now),
        dict(name='gs8', score=91, order_score=91, date=now, doc={'a': 4}),
        dict(name='gs9', score=92, order_score=92, date=now, ids=[9]),
    ]
    Test.batch_write(items, overwrite=True)
    item8 = Test.get(name='gs8', score=91)
    print item8.name, item8.ids, item8.doc, item8.score
    primary_keys = [
        {'name': 'gs8', 'score': 91},
        {'name': 'gs9', 'score': 92},
        {'name': 'gs9', 'score': 99},
    ]
    _items = Test.batch_get(*primary_keys)
    for item in _items:
        print item.name, item.score, item.order_score, type(item.doc)


def init_data():
    for i in xrange(2000):
        print i
        Test.create(name='gs%s' % i,
                    score=i, order_score=99.99,
                    date=now)


def query():
    query = Test.query(Test.name, Test.score, Test.order_score)
    print query
    print query.projections


def main():
    # delete_table()
    # create_table()
    # show_table()
    # update_table()
    # show_table()
    create_and_get_item()
    # batch_add_and_get_item()
    # delete_item()
    # init_data()
    # query()


if __name__ == '__main__':
    main()
