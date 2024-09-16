# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 13:25:07 2023

@author: C.Z.J
"""
import MySQLdb as MS
from sqlalchemy import create_engine
import pymysql
import pandas as pd

class Mysql:
    """
    include multiple ways needed when 
    1. importing dataframe into mysql
       1.1 no match db, table, create one
       1.2 match table, append/change/create new
    2. geting needed data from mysql
    3. managing mysql
       3.1 create/delete db
       3.2 create/delete table
    """
    ### import necessary packages
    import MySQLdb as MS
    from sqlalchemy import create_engine
    import pymysql
    import pandas as pd
    
    
    ### initialize mysql
    def __init__ (self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        """
        password need to be str
        """
        
        
    ### connect mysql: first way
    def sql_Conn(self):
        conn = MS.connect(host=self.host,
                                   user=self.username,
                                   passwd=self.password,
                                   charset='utf8')
        return conn
    
    # second way
    def sql_Conn_E(self, db_name):
        conn = MS.connect(host=self.host,
                                   user=self.username,
                                   passwd=self.password,
                                   db=db_name,
                                   charset='utf8')
        return conn
    
    ### third way: used to export data from Mysql
    def py_to_Mysql(self, db_name):
        conn = pymysql.connect(host=self.host,
                                   user=self.username,
                                   passwd=self.password,
                                   db=db_name,
                                   charset='utf8')
        return conn
        

    ### create db
    def create_db(self, db_name):
        conn = self.sql_Conn()
        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS %s;' % db_name)
        
        cursor.close()
        conn.close()
        
    
    ### delete db
    def delete_db(self, db_name):
        conn = self.sql_Conn_E(db_name)
        cursor = conn.cursor()
        cursor.execute('DELETE DATABASE IF EXISTS %s;' % db_name)
        
        cursor.close()
        conn.close()
        
        
    ### create table
    def create_table(self, db_name, code):
        conn = self.sql_Conn_E(db_name)
        cursor = conn.cursor()
        cursor.execute(code)
        
        cursor.close()
        conn.close()
        
    
    ### delete table
    def create_table(self, db_name, table_name):
        conn = self.sql_Conn_E(db_name)
        cursor = conn.cursor()
        cursor.execute('DELETE TABLE IF EXISTS %s;' % table_name)
        
        cursor.close()
        conn.close()
        
    ### insert data by dataframe
    def insert_DF(self, db_name, dataframe, table_name, method='append'):
        """

        Parameters
        ----------
        :param method: {'fail', 'replace', 'append'}, default 'fail'
            - fail: If table exists, do nothing.
            - replace: If table exists, drop it, replace it, and insert data.
            - append: If table exists, insert data. Create if does not exists.
        Returns
        -------
        None.

        """
        db_info = {'user': self.username,
                   'password': self.password,
                   'host': self.host,
                   'port': self.port,
                   'database': db_name}
        
        engine = create_engine(
            'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % db_info, 
            encoding='utf-8')
        
        dataframe.to_sql(table_name, engine, if_exists=method, index=False)
        
        
    ### insert data
    def insert_data_Multi(self, db_name, dataframe, table_name):
        
        # get connection
        conn = self.sql_Conn_E(db_name)
        cursor = conn.cursor()
        
        # get value by column
        keys = dataframe.keys()
        values = dataframe.values.tolist()
        
        key_sql = ','.join(keys)
        value_sql = ','.join(['%s'] * dataframe.shape[1])
        
        # SQL code
        insert_data_str = """insert into %s (%s) values (%s)""" % (table_name, key_sql, value_sql)
        
        # execute code
        cursor.executemany(insert_data_str, values)
        conn.commit()
        
        # shut connection
        cursor.close()
        conn.close()
        
    
    ### update data
    def update_data_multi(self, db_name, dataframe, table_name):
        
        # get connection
        conn = self.sql_Conn_E(db_name)
        cursor = conn.cursor()
        
        # get value by column
        keys = dataframe.keys()
        values = dataframe.values.tolist()
        
        key_sql = ','.join(keys)
        value_sql = ','.join(['%s'] * dataframe.shape[1])
        
        # SQL code
        insert_data_str = """insert into %s (%s) values (%s) ON DUPLICATE KEY UPDATE""" % (table_name, 
                                                                                           key_sql, 
                                                                                           value_sql)
        update_str = ','.join(["{key} = VALUES({key})".format(key=key) for key in keys])
        insert_data_str += update_str
        
        # execute code
        cursor.executemany(insert_data_str, values)
        conn.commit()
        
        # shut connection
        cursor.close()
        conn.close()
        
    
    ### get specific data from MySQL
    def get_data(self, db_name, sql_code, column):
        """

        Parameters
        ----------
        sql_code Type: str
                 Description: MySql code
        column   Type: list or str
                 Exanple: 1. ['column_1', 'column_2',...]; 2. '*'
        -------
        No return.

        """
        # get connection
        conn = self.py_to_Mysql(db_name)
        cursor = conn.cursor()
        
        # get data
        cursor.execute(sql_code)
        data_mysql = cursor.fetchall()
        
        global dfsql
        if column == '*':
            column=[col[0] for col in cursor.description]
            fsql = pd.DataFrame(list(data_mysql), columns=column) # In this case, get all columns
        else:
            fsql = pd.DataFrame(list(data_mysql), columns=column) # In this case, get selected columns
            
        dfsql = pd.DataFrame(list(data_mysql), columns=column)
        
        # shut connection
        cursor.close()
        conn.close()
        
        
        
        
        
        
        
        
        
