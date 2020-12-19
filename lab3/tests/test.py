import logging
from py2sqlm import Py2SQL

logging.basicConfig(level='DEBUG')

if __name__ == '__main__':
   py2sql = Py2SQL()
   py2sql.db_connect(
      host='localhost',
      database='country',
      user='postgres',
      password='password',
      port=5432
   )
   logging.info(f'Database version: {py2sql.db_engine}')
   logging.info(f'Database name: {py2sql.db_name}')
   logging.info(f'Database size: {py2sql.db_size} mb')
   logging.info(f'Database tables: {py2sql.db_tables}')
   logging.info(f'Table city structure: {py2sql.db_table_structure("city")}')
   logging.info(f'Table city size: {py2sql.db_table_size("city")} mb')
   py2sql.db_disconnect()
