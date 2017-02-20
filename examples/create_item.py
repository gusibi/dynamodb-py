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
    - create
    - save
    - batch_write
'''

db3 = boto3.resource('dynamodb', region_name='us-west-2',
                      endpoint_url="http://localhost:8000")


def create_item_by_boto3():
    table = db3.Table('Movies')
    with open("moviedata.json") as json_file:
        movies = json.load(json_file, parse_float=decimal.Decimal)
        for movie in movies[:10]:
            year = int(movie['year'])
            title = movie['title']
            info = movie['info']
            rating = movie['info'].get('rating', 0)
            rank = movie['info'].get('rank', 0)
            release_date = movie['info'].get('release_date')

            print("Adding movie:", year, title)
            item={
                'year': year,
                'title': title,
                'info': info,
                'rating': rating,
                'rank': rank,
                'release_date': release_date
            }
            print(item)
            table.put_item(Item=item)
        item_count = db3.meta.client.describe_table(
            TableName='Movies')['Table']['ItemCount']
        print('Movies item count: %s' % item_count)


def create_item():
    with open("moviedata.json") as json_file:
        movies = json.load(json_file)
        for movie in movies[10:200]:
            release_date = movie['info'].get('release_date')
            release_date = str_to_time(release_date)
            result = dict(
                year=int(movie['year']),
                title=movie['title'],
                info=movie['info'],
                rating=movie['info'].get('rating', 0.0),
                rank=movie['info'].get('rank', 0),
                release_date = release_date
            )
            print(movie['title'], int(movie['year']))
            print(result)
            Movies.create(**result)
        item_count = Movies.item_count()
        print('Movies item count: %s' % item_count)


def batch_add_items():
    with open("moviedata.json") as json_file:
        movies = json.load(json_file)
        count = len(movies)
        offset, limit = 200, 100
        while True:
            if count < offset:
                break
            items = []
            for movie in movies[offset:offset+limit]:
                release_date = movie['info'].get('release_date')
                release_date = str_to_time(release_date)
                result = dict(
                    year=int(movie['year']),
                    title=movie['title'],
                    info=movie['info'],
                    rating=movie['info'].get('rating', 0.0),
                    rank=movie['info'].get('rank', 0),
                    release_date = release_date
                )
                print(result)
                items.append(result)
            Movies.batch_write(items)
            item_count = Movies.item_count()
            print('Movies item count: %s' % item_count)
            offset += limit


if __name__ == '__main__':
    # create_item_by_boto3()
    # create_item()
    batch_add_items()
