from __future__ import absolute_import

from .cursor import Cursor
from .proxy import Proxy
from .log import logger
#import sys
#import importlib,sys
#reload(sys)
#sys.setdefaultencoding('utf-8')

class Connection(object):
    def __init__(self, username, password, endpoint, project, **kwargs):
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.project = project
        self.proxy = Proxy(self.endpoint)
        self.limit = kwargs['limit'] if 'limit' in kwargs else 50000
        self.proxy.login(self.username, self.password)

    def close(self):
        logger.debug('Connection close called')

    def commit(self):
        logger.warn('Transactional commit is not supported')

    def rollback(self):
        logger.warn('Transactional rollback is not supported')

    def list_tables(self):
        route = 'tables_and_columns'
        params = {'project': self.project}
        tables = self.proxy.get(route, params=params)
        tableNames = [t['table_NAME'] for t in tables]
        return tableNames

    def list_columns(self, table_name):
        table_NAME = str(table_name).upper()
        route = 'tables_and_columns'
        params = {'project': self.project}
        tables = self.proxy.get(route, params=params)
        table = [t for t in tables if t['table_NAME'] == table_NAME][0]
        return table['columns']

    def list_schemas(self):   
        route = 'tables_and_columns'
        params = {'project': self.project}
        tables = self.proxy.get(route, params=params)
        schema_names = [t['table_SCHEM'] for t in tables]
        logger.info("schema_names is : "+str(schema_names))
        return schema_names

    def cursor(self):
        return Cursor(self)

def connect(username='', password='', endpoint='', project='', **kwargs):
    return Connection(username, password, endpoint, project, **kwargs)
