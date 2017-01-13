#! -*- coding: utf-8 -*-

from os import environ
import boto3


class ConnectionManager:

    def __init__(self, mode=None, config=None, endpoint=None,
                 port=None, use_instance_metadata=False):
        self.db = None
        if mode == "local":
            if config is not None:
                raise Exception('Cannot specify config when in local mode')
            endpoint = endpoint or 'localhost'
            port = port or '8000'
            self.db = self.getDynamoDBConnection(
                endpoint=endpoint, port=port, local=True)
        elif mode == "service":
            self.db = self.getDynamoDBConnection(
                config=config,
                endpoint=endpoint,
                use_instance_metadata=use_instance_metadata)
        else:
            raise Exception("Invalid arguments, please refer to usage.")

    def getDynamoDBConnection(self, config=None, endpoint=None, port=None,
                              local=False, use_instance_metadata=False):
        if not config:
            config = {'region_name': 'us-west-2'}
        params = {
            'region_name': config.get('region_name', 'cn-north-1')
        }
        if local:
            endpoint_url = 'http://{endpoint}:{port}'.format(endpoint=endpoint,
                                                             port=port)
            params['endpoint_url'] = endpoint_url
            db = boto3.resource('dynamodb', **params)
        else:
            if not config or not isinstance(config, dict):
                raise Exception("Invalid config")
            params.update(config)
            db = boto3.resource('dynamodb', **params)
        return db


config = {
    'aws_access_key_id': environ.get('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': environ.get('AWS_SECRET_ACCESS_KEY'),
    'region_name': environ.get('REGION_NAME', 'cn-north-1')
}


db = ConnectionManager(mode='service', config=config).db
db_local = ConnectionManager(mode='local').db
