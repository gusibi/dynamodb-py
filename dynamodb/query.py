#! -*- coding: utf-8 -*-

from .adapter import Table
from .fields import Fields
from .errors import FieldValidationError


class Query(object):

    def __init__(self, model_object, *args, **kwargs):
        self.model_object = model_object
        self.model_class = self.model_object.__class__
        self.ProjectionExpression = self._projection_expression(*args)

    def _projection_expression(self, *args):
        instance = self.model_object
        for arg in args:
            if isinstance(arg, Fields):
                name = arg.name
                print instance.fields
                print instance
                if arg not in instance.fields:
                    raise FieldValidationError('%s not found' % name)
                instance.projections.append(name)
            else:
                raise FieldValidationError('Bad type must be Attribute type')
        return instance

    def get(self, **primary_key):
        # get directly by primary key
        instance = self.model_class(**primary_key)
        item = Table(instance).get()
        if not item:
            return None
        value_for_read = instance._get_values_for_read(item)
        return self.model_class(**value_for_read)

    def where(self, *args):
        # Find by any number of matching criteria... though presently only
        # "where" is supported.
        pass
