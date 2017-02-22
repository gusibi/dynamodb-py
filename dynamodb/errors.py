#! -*- coding: utf-8 -*-
'''
All the errors specific to Dynamodb.  The goal is to mimic ActiveRecord.
'''
from __future__ import unicode_literals

__all__ = ['ClientException', 'ConnectionException', 'ParameterException',
           'FieldValidationException', 'ValidationException']


class ValidationException(Exception):
    pass


class ClientException(Exception):
    pass


class ConnectionException(Exception):
    pass


class ParameterException(Exception):
    pass


class FieldValidationException(Exception):

    def __init__(self, errors, *args, **kwargs):
        super(FieldValidationException, self).__init__(*args, **kwargs)
        self._errors = errors

    @property
    def errors(self):
        return self._errors

    def __str__(self):
        return self._errors


class NotFoundError(Exception):

    def __init__(self, errors, *args, **kwargs):
        super(NotFoundError, self).__init__(*args, **kwargs)
        self._errors = errors

    @property
    def errors(self):
        return self._errors

    def __str__(self):
        return self._errors
