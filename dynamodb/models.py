#! -*- coding: utf-8 -*-

from .fields import Attribute
from .errors import FieldValidationError


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
    model_class._indexed_value_fields = []
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
            elif v.index_value:
                model_class._indexed_value_fields.append(k)
    if model_class._meta['indexed_fields']:
        model_class._indexed_fields.extend(model_class._meta['indexed_fields'])


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
        # _initialize_indices(cls, name, bases, attrs)


class ModelBase(object):

    __metaclass__ = ModelMetaclass

    @classmethod
    def create_table(cls):
        pass

    @classmethod
    def drop_table(cls):
        pass

    @classmethod
    def batch_get(cls):
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

    def save(self, overwrite=False):
        self.is_valid()
        pass


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
        self._errors = []
        for field in self.fields:
            # print field, self
            try:
                field.validate(self)
            except FieldValidationError as e:
                self._errors.extend(e.errors)
        self.validate()
        return not bool(self._errors)

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
