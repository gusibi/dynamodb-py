## dynamodb-py

dynamodb-py is an ORM for Amazon's DynamoDB for Python applications. It provides similar functionality to ActiveRecord and improves on Amazon's existing HashModel by providing better searching tools and native association support.

DynamoDB is not like other document-based databases you might know, and is very different indeed from relational databases. It sacrifices anything beyond the simplest relational queries and transactional support to provide a fast, cost-efficient, and highly durable storage solution. If your database requires complicated relational queries and transaction support, then this modest Gem cannot provide them for you, and neither can DynamoDB. In those cases you would do better to look elsewhere for your database needs.

## Requires

```
* boto3
* pytz
* dateutil
```

## Installation

## Setup

## Table

ynamodb-py has some sensible defaults for you when you create a new table, including the table name and the primary key column. But you can change those if you like on table creation.

```
from dynomadb import Model
from dynomadb.adapter import Table


class Test(Model):

    __table_name__ = 'test'

    ReadCapacityUnits = 100
    WriteCapacityUnits = 120

    name = CharField(name='name', hash_key=True)
    score = IntegerField(name='score', range_key=True)

# name and score is primary key

# create_table

Table(Test()).create()

```

## Fields
You'll have to define all the fields on the model and the data type of each field. Every field on the object must be included here; if you miss any they'll be completely bypassed during DynamoDB's initialization and will not appear on the model objects.

#### IntegerField

Stores an int. 

#### DateTimeField

Can store a TimeDelta object. Saved in DynamoDB as a ISO 8601 string.

#### DateField

Can store a TimeDelta object. Saved in DynamoDB as a ISO 8601 string.

#### TimeDeltaField

Can store a TimeDelta object. Saved in DynamoDB as a ISO 8601 string.

#### FloatField

Can store floats.

#### BooleanField

Can store bools. Saved in dynamodb as False and True.

#### ListField

Can store a list of unicode, int, float, as well as other dynamodb-py models.

#### DictField

Can store a dict. 

```
from dynomadb import Model


class Test(Model):

    __table_name__ = 'test'

    ReadCapacityUnits = 100
    WriteCapacityUnits = 120

    name = CharField(name='name', hash_key=True)
    score = IntegerField(name='score', range_key=True)
    order_score = FloatField(name='order_score', default=0.0)
    date = DateTimeField(name='date')
    ids = ListField(name='ids', default=[])
    doc = DictField(name='doc', default={})

```

You can optionally set a default value on a field using either a plain value or a lambda


## Usage

```
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
    Test.create(name='gs2', score=100, order_score=99.99, date=now)
    Test.create(name='gs4', score=100, order_score=99.99, date=now, ids=[1, 2,3])
    Test.create(name='gs5', score=100, order_score=99.99, date=now, doc={'a': 1.1})
    item1 = Test.get(name='gs1', score=100)
    print item1.name, item1.ids, item1.doc


def delete_item():
    Test.create(name='gs6', score=100, order_score=99.99, date=now, ids=[1,2,4, 101], doc={'a': 1.1})
    item6 = Test.get(name='gs6', score=100)
    print item6.name, item6.ids, item6.doc
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
```


## Querying

## Credits

dynamodb borrows code, structure, and even its name very liberally from the truly amazing Dynamoid and redisco.
