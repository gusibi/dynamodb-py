#! -*- coding: utf-8 -*-
import copy

import six
from six import with_metaclass
from botocore.exceptions import ClientError

from .table import Table
from .query import Query, GlobalQuery
from .fields import Attribute
from .errors import FieldValidationException, ValidationException, ClientException
from .helpers import get_items_for_storage, cache_for


def _initialize_attributes(model_class, name, bases, attrs):
    """
    Initialize the attributes of the model.
    """
    model_class._attributes = {}

    # In case of inheritance, we also add the parent's
    # attributes in the list of our attributes
    for parent in bases:
        if not isinstance(parent, ModelMetaclass):
            continue
        for k, v in six.iteritems(parent._attributes):
            model_class._attributes[k] = v

    for k, v in six.iteritems(attrs):
        if isinstance(v, Attribute):
            model_class._attributes[k] = v
            v.name = v.name or k


def _initialize_indexes(model_class, name, bases, attrs):
    """
    Stores the list of indexed attributes.
    """
    model_class._local_indexed_fields = []
    model_class._local_indexes = {}
    model_class._global_indexed_fields = []
    model_class._global_indexes = model_class.__global_indexes__
    model_class._global_secondary_indexes = []
    model_class._hash_key = None
    model_class._range_key = None
    for parent in bases:
        if not isinstance(parent, ModelMetaclass):
            continue
    for k, v in six.iteritems(attrs):
        if isinstance(v, (Attribute,)):
            if v.indexed:
                model_class._local_indexed_fields.append(k)

    # setting hash_key and range_key and local indexes 
    for k, v in six.iteritems(attrs):
        if isinstance(v, (Attribute,)):
            if v.indexed:
                model_class._local_indexed_fields.append(k)
                model_class._local_indexes[k] = '{table_name}_ix_{field}'.format(
                    table_name=name, field=k)
            elif v.range_key:
                model_class._range_key = k
            elif v.hash_key:
                model_class._hash_key = k
    if name not in ('ModelBase', 'Model') and not model_class._hash_key:
        raise ValidationException('hash_key is required')

    # setting global_indexes
    global_indexes = []
    global_fields = set()
    for gindex in model_class._global_indexes:
        index = {}
        name, primary_key, fields = gindex
        index['name'] = name
        # 处理主键
        if len(primary_key) == 2:
            hash_key, range_key = primary_key
        elif len(primary_key) == 1:
            hash_key, range_key = primary_key[0], None
        else:
            raise ValidationException('invalid primary key')
        index['hash_key'] = hash_key
        global_fields.add(hash_key)
        index['range_key'] = range_key
        if range_key:
            global_fields.add(range_key)
        if hash_key not in model_class._attributes:
            raise ValidationException('invalid hash key: %s' % hash_key)
        if range_key and range_key not in model_class._attributes:
            raise ValidationException('invalid range key: %s' % range_key)
        # 处理备份键
        for field in fields:
            if field not in model_class._attributes:
                raise ValidationException('invalid include field: %s' % field)
        index['include_fields'] = fields
        global_indexes.append(index)
    model_class._global_indexed_fields = global_fields
    model_class._global_secondary_indexes = global_indexes


class ModelMetaclass(type):

    """
    Metaclass of the Model.
    """

    __table_name__ = None
    __global_indexes__ = []
    __local_index__ = {}

    def __init__(cls, name, bases, attrs):
        super(ModelMetaclass, cls).__init__(name, bases, attrs)
        name = cls.__table_name__ or name
        _initialize_attributes(cls, name, bases, attrs)
        _initialize_indexes(cls, name, bases, attrs)


class ModelBase(with_metaclass(ModelMetaclass, object)):

    #  __metaclass__ = ModelMetaclass

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        if not instance.is_valid():
            raise ValidationException(instance.errors)
        try:
            Table(instance).put_item(instance.item)
            item = cls(**instance.item)
            return item
        except ClientError as e:
            raise ClientException(e.response['Error']['Message'])

    def condition(self, *args):
        instance = copy.deepcopy(self)
        ConditionExpression = None
        ExpressionAttributeValues = {}
        for field_inst, exp, is_key in args:
            if is_key:
                pass
            else:
                if not ConditionExpression:
                    ConditionExpression = exp
                else:
                    ConditionExpression = ConditionExpression & exp
        instance.ConditionExpression = ConditionExpression
        instance.ExpressionAttributeValues = ExpressionAttributeValues
        return instance

    def _prepare_update_item_params(self, **kwargs):
        params = kwargs
        ConditionExpression = getattr(self, 'ConditionExpression', None)
        ExpressionAttributeValues = getattr(self, 'ExpressionAttributeValues', {})
        if ConditionExpression:
            params['ConditionExpression'] = ConditionExpression
        if ExpressionAttributeValues:
            params['ExpressionAttributeValues'] = ExpressionAttributeValues
        return params

    def update(self, *args, **kwargs):
        update_fields = {}
        ReturnValues = kwargs.pop('ReturnValues', 'ALL_NEW')
        ReturnConsumedCapacity = kwargs.pop('ReturnConsumedCapacity', 'NONE')
        params = self._prepare_update_item_params(
            ReturnValues=ReturnValues,
            ReturnConsumedCapacity=ReturnConsumedCapacity)
        if not self.validate_attrs(**kwargs):
            raise FieldValidationException(self._errors)
        for k, v in six.iteritems(kwargs):
            field = self.attributes[k]
            update_fields[k] = field.typecast_for_storage(v)
        # use storage value
        item = Table(self).update_item(
            update_fields, *args, **params)
        value_for_read = self._get_values_for_read(item)
        for k, v in value_for_read.items():
            setattr(self, k, v)
        return self

    @classmethod
    def get(cls, **primary_key):
        instance = cls(**primary_key)
        item = Table(instance).get_item()
        if not item:
            return None
        value_for_read = instance._get_values_for_read(item)
        return cls(**value_for_read)

    @classmethod
    def batch_get(cls, *primary_keys):
        instance = cls()
        items = Table(instance).batch_get_item(*primary_keys)
        results = []
        for item in items:
            value_for_read = instance._get_values_for_read(item)
            results.append(cls(**value_for_read))
        return results

    @classmethod
    def batch_write(cls, kwargs, overwrite=False):
        '''
        kwargs: items list
        '''
        items = get_items_for_storage(cls, kwargs)
        instance = cls()
        return Table(instance).batch_write(items, overwrite=overwrite)

    def delete(self):
        # delete an item
        return Table(self).delete_item()

    @classmethod
    def query(cls, *args):
        instance = cls()
        return Query(instance, *args)

    @classmethod
    def global_query(cls, index_name=None, *args):
        instance = cls()
        return GlobalQuery(instance, index_name, *args)

    @classmethod
    def scan(cls):
        instance = cls()
        return Table(instance).scan()

    @classmethod
    def item_count(cls):
        instance = cls()
        return Table(instance).item_count()

    def write(self):
        item = Table(self).put_item(self.item)
        return item

    def save(self, overwrite=False):
        if not self.is_valid():
            raise ValidationException(self.errors)
        self.write()
        return self


class Model(ModelBase):

    def __init__(self, **kwargs):
        self.update_attributes(**kwargs)
        self.projections = []

    def is_valid(self):
        """
        Returns True if all the fields are valid, otherwise
        errors are in the 'errors' attribute
        It first validates the fields (required, unique, etc.)
        and then calls the validate method.
        >>> from dynamodb import Model
        >>> def validate_me(field, value):
        ...     if value == "Invalid":
        ...         return (field, "Invalid value")
        ...
        >>> class Foo(Model):
        ...     bar = Attribute(validator=validate_me)
        ...
        >>> f = Foo()
        >>> f.bar = "Invalid"
        >>> f.save()
        False
        >>> f.errors
        ['bar', 'Invalid value']
        .. WARNING::
        You may want to use ``validate`` described below to validate your model
        """
        self.errors = []
        for field in self.fields:
            try:
                field.validate(self)
            except FieldValidationException as e:
                self.errors.extend(e.errors)
        self.validate()
        return not bool(self.errors)

    def validate_attrs(self, **kwargs):
        self._errors = []
        for attr, value in six.iteritems(kwargs):
            field = self.attributes.get(attr)
            if not field:
                raise ValidationException('Field not found: %s' % attr)
            instance = copy.deepcopy(self)
            setattr(instance, attr, value)
            try:
                field.validate(instance)
            except FieldValidationException as e:
                self._errors.extend(e.errors)
        return not bool(self._errors)

    def validate(self):
        """
        Overriden in the model class.
        The function is here to help you validate your model.
        The validation should add errors to self._errors.
        Example:
        >>> from zaih_core.redis_index import Model
        >>> class Foo(Model):
        ...     name = Attribute(required=True)
        ...     def validate(self):
        ...         if self.name == "Invalid":
        ...             self._errors.append(('name', 'cannot be Invalid'))
        ...
        >>> f = Foo(name="Invalid")
        >>> f.save()
        False
        >>> f.errors
        [('name', 'cannot be Invalid')]
        """
        pass

    def update_attributes(self, **kwargs):
        """
        Updates the attributes of the model.
        >>> class Foo(Model):
        ...    name = Attribute()
        ...    title = Attribute()
        ...
        >>> f = Foo(name="Einstein", title="Mr.")
        >>> f.update_attributes(name="Tesla")
        >>> f.name
        'Tesla'
        """
        params = {}
        attrs = self.attributes.values()
        for att in attrs:
            if att.name in kwargs:
                att.__set__(self, kwargs[att.name])
                params[att.name] = kwargs[att.name]
        return params

    @property
    def attributes(self):
        """Return the attributes of the model.
        Returns a dict with models attribute name as keys
        and attribute descriptors as values.
        """
        return dict(self._attributes)

    @property
    def fields(self):
        """Returns the list of field names of the model."""
        return self.attributes.values()

    def _get_values_for_read(self, values):
        read_values = {}
        for att, value in six.iteritems(values):
            if att not in self.attributes:
                continue
            descriptor = self.attributes[att]
            _value = descriptor.typecast_for_read(value)
            read_values[att] = _value
        return read_values

    def _get_values_for_storage(self):
        data = {}
        if not self.is_valid():
            raise FieldValidationException(self.errors)
        for attr, field in six.iteritems(self.attributes):
            value = getattr(self, attr)
            if value is not None:
                data[attr] = field.typecast_for_storage(value)
        return data

    @property
    def item(self):
        return self._get_values_for_storage()
