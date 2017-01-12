#! -*- coding: utf-8 -*-
'''
All the errors specific to Dynamodb.  The goal is to mimic ActiveRecord.
'''
from __future__ import unicode_literals

__all__ = ['FieldValidationError', 'ParameterError', 'NotFoundError', 'Error']


class Error(Exception):

    def __init__(self, errors, *args, **kwargs):
        self._errors = errors

    def __str__(self):
        return repr(self._errors)


class FieldValidationError(Exception):

    def __init__(self, errors, *args, **kwargs):
        super(FieldValidationError, self).__init__(*args, **kwargs)
        self._errors = errors

    @property
    def errors(self):
        return self._errors

    def __str__(self):
        if self._errors:
            if isinstance(self._errors, list):
                error = self._errors[0]
            else:
                error = self._errors
            return str(error)
        return self._errors


class ParameterError(Exception):

    def __init__(self, errors, *args, **kwargs):
        super(ParameterError, self).__init__(*args, **kwargs)
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
