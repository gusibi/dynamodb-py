#! -*- coding: utf-8 -*-

from __future__ import print_function  # Python 2/3 compatibility
import json
import boto3
import decimal

from movies import Movies

from dynamodb.helpers import str_to_time

'''
create and get item

create:
    - update_item
'''

db3 = boto3.resource('dynamodb', region_name='us-west-2',
                      endpoint_url="http://localhost:8000")

title = "Die Hard 2"
year = 1990


def update_item():
    item = Movies.get(year=year, title=title)
    print("GetItem succeeded:", item.info, item.year)

    item.update(rank=2467, rating=7.1)
    assert item.rank == 2467
    assert item.rating == 7.1

    item.update(Movies.rank.set(2000, attr_label='r'))
    assert item.rank == 2000

    item.update(Movies.rating.set(7.9, if_not_exists='info.rating'))
    assert item.rating == 7.1

    item.update(Movies.rating.set(7.9, if_not_exists='info.rat'))
    assert item.rating == 7.9

    item.update(Movies.info.set(['genres'], list_append=('info.genres', -1)))
    print(item.info['genres'])


if __name__ == '__main__':
    # update_item_by_boto3()
    update_item()
