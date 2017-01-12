#! -*- coding: utf-8 -*-

from datetime import datetime

from dynamodb.models import Model
from dynamodb.fields import CharField, IntegerField, FloatField, DateTimeField


class Test(Model):

    __table_name__ = 'test'

    name = CharField(name='name')
    score = IntegerField(name='score')
    order_score = FloatField(name='order_score')
    date = DateTimeField(name='date')

now = datetime.now()


def main():
    # test = Test(name='gs', score=100, order_score=99.99, date=now)
    # test.save()
    # print '>>>>>>>>>>>>'
    test = Test(name='gs', score='100', order_score=99.99, date=now)
    test.save()


if __name__ == '__main__':
    main()
