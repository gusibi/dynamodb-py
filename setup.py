#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name="dynamodb-py",
      version="0.1.01.12",
      description="A simple DynamoDB ORM for python, based on boto3",
      license="BSD",
      install_requires=["simplejson", "six", "chardet"],
      author="gusibi",
      author_email="cacique1103@gmail.com",
      url="https://github.com/gusibi/dynamodb-py",
      download_url="https://github.com/gusibi/dynamodb-py/archive/master.zip",
      packages=find_packages(),
      keywords=["dynamodb", "amazon", "orm", "database", "nosql"],
      zip_safe=True)
