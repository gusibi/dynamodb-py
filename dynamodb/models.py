#! -*- coding: utf-8 -*-

from .fields import Attribute
from .errors import FieldValidationError
from .adapter import Table


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
        for k, v in parent._attributes.iteritems():
            model_class._attributes[k] = v

    for k, v in attrs.iteritems():
        if isinstance(v, Attribute):
            model_class._attributes[k] = v
            v.name = v.name or k


def _initialize_indices(model_class, name, bases, attrs):
    """
    Stores the list of indexed attributes.
    """
    model_class._indexed_fields = []
    model_class._indexed_unique_fields = model_class.__unique_index__
    model_class._hash_key = None
    model_class._range_key = None
    for parent in bases:
        if not isinstance(parent, ModelMetaclass):
            continue
        for k, v in parent._attributes.iteritems():
            if v.indexed:
                model_class._indexed_fields.append(k)

    for k, v in attrs.iteritems():
        if isinstance(v, (Attribute,)):
            if v.indexed:
                model_class._indexed_fields.append(k)
            elif v.range_key:
                model_class._range_key = k
            elif v.hash_key:
                model_class._hash_key = k


class ModelMetaclass(type):

    """
    Metaclass of the Model.
    """

    __table_name__ = None
    __unique_index__ = []

    def __init__(cls, name, bases, attrs):
        super(ModelMetaclass, cls).__init__(name, bases, attrs)
        name = cls.__table_name__ or name
        _initialize_attributes(cls, name, bases, attrs)
        _initialize_indices(cls, name, bases, attrs)


class ModelBase(object):

    __metaclass__ = ModelMetaclass

    @classmethod
    def create_table(cls):
        instance = cls()
        table = Table(instance).create_table()
        return table

    @classmethod
    def delete_table(cls):
        instance = cls()
        table = Table(instance).delete_table()
        return table

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        return instance.save()

    def update(self, **kwargs):
        pass

    @classmethod
    def get(cls, **primary_keys):
        instance = cls()
        item = Table(instance).get(**primary_keys)
        if not item:
            return None
        return cls(**item)

    @classmethod
    def batch_get(cls, primary_keys_list):
        pass

    @classmethod
    def batch_write(cls):
        pass

    @classmethod
    def delete(cls):
        pass

    @classmethod
    def query(cls):
        pass

    @classmethod
    def scan(cls):
        instance = cls()
        return Table(instance).scan()

    def write(self):
        return Table(self).write(self.item)

    def save(self, overwrite=False):
        if not self.is_valid():
            raise Exception(self.errors)
        self.write()
        return True


class Model(ModelBase):

    def __init__(self, **kwargs):
        self.update_attributes(**kwargs)

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
            except FieldValidationError as e:
                self.errors.extend(e.errors)
        self.validate()
        return not bool(self.errors)

    def validate_attrs(self, **kwargs):
        self._errors = []
        for attr, value in kwargs.iteritems():
            field = self.attributes.get(attr)
            try:
                field.validate(self)
            except FieldValidationError as e:
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
        for att, value in values.iteritems():
            if att not in self.attributes:
                continue
            descriptor = self.attributes[att]
            _value = descriptor.typecast_for_read(value)
            read_values[att] = _value
        return read_values

    @property
    def item(self):
        data = {}
        for attr, field in self.attributes.iteritems():
            value = getattr(self, attr)
            data[attr] = field.typecast_for_storage(value)
        return data

    @property
    def items(self):
        pass
