#! -*- coding: utf-8 -*-

from datetime import datetime

import decimal

from dynamodb.models import Model
from dynamodb.fields import (CharField, IntegerField, FloatField, Attribute,
                             DateTimeField, DictField, ListField)
from dynamodb.adapter import Table


class Test(Model):

    __table_name__ = 'test'
    ReadCapacityUnits = 10
    WriteCapacityUnits = 10

    realname = CharField(name='realname', hash_key=True)
    score = IntegerField(name='score', range_key=True)
    order_score = FloatField(name='order_score', indexed=True)
    category = CharField(name='category', default=' ')
    date_created = DateTimeField(name='date_created', indexed=True)
    ids = ListField(name='ids', default=[])
    doc = DictField(name='doc', default={})

now = datetime.now()


def show_table():
    table_info = Table(Test()).info()
    return table_info


def create_table():
    Table(Test()).create()


def update_table():
    Table(Test()).update()


def delete_table():
    Table(Test()).delete()


def save_item():
    test = Test(realname='gs', score=100, order_score=99.99, date_created=now)
    test.save()


def create_and_get_item():
    Test.create(realname='gs1', score=100, order_score=99.99, date_created=now)
    item1 = Test.get(realname='gs1', score=100)
    print item1.realname, item1.ids, item1.doc, item1.category
    Test.create(realname='gs2', score=100, order_score=99.99, date_created=now)
    Test.create(realname='gs4', score=100, order_score=99.99, date_created=now, ids=[1, 2,3])
    Test.create(realname='gs5', score=100, order_score=99.99, date_created=now, doc={'a': 1.1})


def delete_item():
    Test.create(realname='gs6', score=100, order_score=99.99, date_created=now, ids=[1,2,4, 101], doc={'a': 1.1})
    item6 = Test.get(realname='gs6', score=100)
    print item6.realname, item6.ids, item6.doc
    print 'delete: >>>>>>>>>>>>>'
    item6.delete()
    print 'DeleteItem succeeded: >>>>>>>>>>>'
    item6 = Test.get(realname='gs6', score=100)
    if item6:
        print item6.realname, item6.ids, item6.doc
    else:
        print 'item6 not found'


def batch_add_and_get_item():
    items = [
        dict(realname='gs7', score=90, order_score=90, date_created=now),
        dict(realname='gs7', score=90, order_score=90, date_created=now),
        dict(realname='gs8', score=91, order_score=91, date_created=now, doc={'a': 4}),
        dict(realname='gs9', score=92, order_score=92, date_created=now, ids=[9]),
    ]
    Test.batch_write(items, overwrite=True)
    item8 = Test.get(realname='gs8', score=91)
    print item8.realname, item8.ids, item8.doc, item8.score
    primary_keys = [
        {'realname': 'gs8', 'score': 91},
        {'realname': 'gs9', 'score': 92},
        {'realname': 'gs9', 'score': 99},
    ]
    _items = Test.batch_get(*primary_keys)
    for item in _items:
        print item.realname, item.score, item.order_score, type(item.doc)


def init_data():
    for i in xrange(200000, 300000):
        if i % 100 == 0:
            print i
        Test.create(realname='gs%s' % i,
                    score=i, order_score=i,
                    date_created=now)


def query_without_index():
    for i in xrange(20, 40):
        Test.create(realname='gs100',
                    score=i, order_score=i,
                    category=str(i),
                    date_created=now)

    query = Test.query(Test.realname, Test.score, Test.order_score, Test.category)
    # item = query.get(realname='gs100', score=34)
    # print item
    # item = query.consistent.get(realname='gs100', score=33)
    # print item
    # query = query.where(Test.realname.eq('gs100'), Test.order_score.lt(34), Test.score.gt(30))
    # items = query.all()
    # for item in items:
    #     print item

    query = query.where(Test.realname.eq('gs100'),
                        Test.order_score.between(22, 26))
    items = query.limit(3).all()
    print 'itmes filter by without index'
    for item in items:
        print item


def query_with_index():
    for i in xrange(20, 40):
        Test.create(realname='gs100',
                    score=i, order_score=i,
                    category=str(i),
                    date_created=now)

    _query = Test.query(Test.realname, Test.score, Test.order_score, Test.category)
    query = (_query
             .where(Test.realname.eq('gs100'), Test.order_score.between(22, 26))
             .order_by(Test.order_score, asc=True))
    print 'itmes filter by without index'
    items = query.limit(3).all()
    for item in items:
        print item


def scan():
    # for i in xrange(200, 240):
    #     Test.create(realname='gs200',
    #                 score=i, order_score=i,
    #                 category=str(i),
    #                 date_created=now)

    query = Test.query(Test.realname, Test.score, Test.order_score, Test.category)
    # item = query.get(realname='gs100', score=34)
    # print item
    # item = query.consistent.get(realname='gs100', score=33)
    # print item
    # query = query.where(Test.realname.eq('gs100'), Test.order_score.lt(34), Test.score.gt(30))
    # items = query.all()
    # for item in items:
    #     print item

    query = query.scan.where(Test.order_score.between(200, 234))
    items = query.limit(2).all()
    for item in items:
        print item


def main():
    # delete_table()
    # create_table()
    # show_table()
    # update_table()
    # show_table()
    # create_and_get_item()
    # batch_add_and_get_item()
    # delete_item()
    # init_data()
    query_without_index()
    query_with_index()
    # scan()


if __name__ == '__main__':
    main()
