## dynamodb-py

dynamodb-py is an ORM for Amazon's DynamoDB for Python applications. It provides similar functionality to ActiveRecord and improves on Amazon's existing HashModel by providing better searching tools and native association support.

DynamoDB is not like other document-based databases you might know, and is very different indeed from relational databases. It sacrifices anything beyond the simplest relational queries and transactional support to provide a fast, cost-efficient, and highly durable storage solution. If your database requires complicated relational queries and transaction support, then this modest Gem cannot provide them for you, and neither can DynamoDB. In those cases you would do better to look elsewhere for your database needs.

## Requires

```
* boto3
* pytz
* dateutil
* simplejson
```

## Installation

```
pip install git+https://github.com/gusibi/dynamodb-py.git@master
```

## Setup

## Table

ynamodb-py has some sensible defaults for you when you create a new table, including the table name and the primary key column. But you can change those if you like on table creation.

```
from dynamodb.model import Model
from dynamodb.fields import CharField, IntegerField, FloatField, DictField
from dynamodb.table import Table

class Movies(Model):

    __table_name__ = 'Movies'

    ReadCapacityUnits = 10
    WriteCapacityUnits = 10

    year = IntegerField(name='year', hash_key=True)
    title = CharField(name='title', range_key=True)
    rating = FloatField(name='rating', indexed=True)
    rank = IntegerField(name='rank', indexed=True)
    release_date = CharField(name='release_date')
    info = DictField(name='info', default={})

# create_table
Table(Movies()).create()

# update_table
Table(Movies()).update()

# delete_table
Table(Movies()).delete()
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
class Movies(Model):

    __table_name__ = 'Movies'

    ReadCapacityUnits = 10
    WriteCapacityUnits = 10

    year = IntegerField(name='year', hash_key=True)
    title = CharField(name='title', range_key=True)
    rating = FloatField(name='rating', indexed=True)
    rank = IntegerField(name='rank', indexed=True)
    release_date = CharField(name='release_date')
    info = DictField(name='info', default={})
```

You can optionally set a default value on a field using either a plain value or a lambda


## Usage

[Example](https://github.com/gusibi/dynamodb-py/tree/master/examples)

## Querying

## Credits

dynamodb borrows code, structure, and even its name very liberally from the truly amazing Dynamoid and redisco.
