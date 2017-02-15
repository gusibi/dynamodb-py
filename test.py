#! -*- coding: utf-8 -*-

from datetime import datetime

import decimal

from dynamodb.model import Model
from dynamodb.fields import (CharField, IntegerField, FloatField, Attribute,
                             DateTimeField, DictField, ListField)
from dynamodb.table import Table


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


def update_item_by_set():
    Test.create(realname='gs01', score=100, order_score=99.99, date_created=now)
    item = Test.get(realname='gs01', score=100)
    item.update(Test.order_score.set(80))
    print 'set'
    assert item.order_score == 80
    item.update(Test.order_score.set(78.7, attr_label=':os'))
    print 'set with attr_label'
    assert item.order_score == 78.7
    item.update(Test.order_score.set(70, if_not_exists=('order_score', 78.7)))
    print 'set with if_not_exists'
    assert item.order_score == 78.7
    item.update(Test.order_score.set(8, if_not_exists=('ids[0]', 10)))
    assert item.order_score == 10
    print 'ids', item.ids, type(item.ids)
    item.update(ids=[12])
    print 'ids', item.ids, type(item.ids)
    item.update(Test.ids.set([100], list_append=('ids', -1)))
    print 'set with list_append'
    assert item.ids[-1] == 100
    item.update(Test.order_score.set(78.7, attr_label=':os'),
                doc={'a': 'bbb'})
    print 'set with attr_label and upate_field'
    assert item.doc['a'] == 'bbb'


def update_item_by_set_func():
    # list_append
    Test.create(realname='gs02', score=101, order_score=9.99, date_created=now)
    item = Test.get(realname='gs02', score=101)
    assert item.order_score == 9.99
    item.update(Test.ids.list_append([1]))
    assert item.ids == [1]
    item.update(Test.ids.list_append([2, 4]))
    assert item.ids == [1, 2, 4]
    item.update(Test.ids.set([8], list_append=('ids', -1)))
    assert item.ids == [1, 2, 4, 8]

    # remove
    item.update(Test.ids.remove(indexes=[2, 4]))
    assert item.ids == [1, 2, 8]
    item.update(Test.ids.remove(indexes=[4, 8, 9]))
    assert item.ids == [1, 2, 8]

    # add
    item.update(Test.order_score.add(4))
    assert item.order_score == 13.99
    item.update(Test.doc.add(4, path='doc.b'))
    assert item.doc['b'] == 4
    item.update(Test.doc.add(4, path='doc.b'))
    assert item.doc['b'] == 8
    item.update(doc={'a': {'11', '22'}})
    item.update(Test.doc.add({'set1'}, path='doc.a'))
    assert 'set1' in item.doc['a']


def create_and_update_item():
    Test.create(realname='gs1', score=100, order_score=99.99, date_created=now)
    item = Test.get(realname='gs1', score=100)
    print item.realname, item.score, item.order_score
    item.update(order_score=90.9)
    assert item.order_score == 90.9
    item.update(Test.order_score.add(1),
                Test.ids.list_append([1, 2]),
                doc={'a': 'aaaa'})
    assert item.order_score == 91.9
    assert item.ids == [1, 2]
    assert item.doc['a'] == 'aaaa'
    item = Test(realname='gs1', score=100).update(order_score=20.0)
    assert item.order_score == 20
    item = Test(realname='gs1', score=100).condition(Test.order_score.eq(10.0)).update(date_created=now)
    print item.realname, item.score, item.order_score, item.date_created


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
    for i in xrange(200000, 201000):
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
    item = query.get(realname='gs100', score=34)
    print item
    item = query.consistent.get(realname='gs100', score=33)
    print item
    query = query.where(Test.realname.eq('gs100'), Test.order_score.lt(34), Test.score.gt(30))
    items = query.all()
    for item in items:
        print item

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
    # update_item_by_set()
    # update_item_by_set_func()
    create_and_update_item()
    # batch_add_and_get_item()
    # delete_item()
    # init_data()
    # query_without_index()
    # query_with_index()
    # scan()


if __name__ == '__main__':
    main()
