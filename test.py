#! -*- coding: utf-8 -*-

from datetime import datetime

import decimal

from dynamodb.models import Model
from dynamodb.fields import (CharField, IntegerField, FloatField,
                             DateTimeField, DictField, ListField)


class Test(Model):

    __table_name__ = 'test'
    ReadCapacityUnits = 12
    WriteCapacityUnits = 12

    name = CharField(name='name', hash_key=True)
    score = IntegerField(name='score', range_key=True)
    order_score = FloatField(name='order_score')
    date = DateTimeField(name='date')
    ids = ListField(name='ids', default=[])
    doc = DictField(name='doc', default={})

now = datetime.now()


def main():
    # Test.create_table()
    test = Test(name='gs', score=100, order_score=99.99, date=now)
    test.save()
    Test.create(name='gs1', score=100, order_score=99.99, date=now)
    Test.create(name='gs2', score=100, order_score=99.99, date=now)
    Test.create(name='gs4', score=100, order_score=99.99, date=now, ids=[1, 2,3])
    Test.create(name='gs5', score=100, order_score=99.99, date=now, doc={'a': decimal.Decimal(str(1.1))})
    Test.create(name='gs6', score=100, order_score=99.99, date=now, ids=[1,2,4, 1.1], doc={'a': 1.1})
    item4 = Test.get(name='gs4', score=100)
    print item4.name, item4.ids, item4.doc
    item5 = Test.get(name='gs5', score=100)
    print item5.name, item5.ids, item5.doc
    item6 = Test.get(name='gs6', score=100)
    print item6.name, item6.ids, item6.doc
    # print Test.scan()
    # Test.delete_table()


if __name__ == '__main__':
    main()
