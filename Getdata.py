# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 10:13:48 2023

@author: C.Z.J

This file means to get historical data of type_A stock in Shanghai, Shenzhen

"""
import pandas as pd
import time
import akshare as ak
import datetime



# compute the time
start1 = time.time()



### 获取今日上证、深证上市A股信息
start = time.time()
stock_info_sh_name_code_df = ak.stock_info_sh_name_code(symbol='主板A股')
stock_info_sh_name_code_df1 = ak.stock_info_sh_name_code(symbol='科创板')
# print(stock_info_sh_name_code_df)
stock_info_sz_name_code_df = ak.stock_info_sz_name_code(indicator='A股列表')
# print(stock_info_sz_name_code_df)

end = time.time()
print('Get List_Running time: %s Seconds'%(end - start))

del start, end


### 存储A股股票上市数据 方便后续比对
stock_info_sh_name_code_df.columns = ['stock_code', 'stock_name', 'company_name', 'public_date'] #重命名爬取的列
stock_info_sh_name_code_df1.columns = ['stock_code', 'stock_name', 'company_name', 'public_date'] #重命名爬取的列
stock_info_sz_name_code_df.columns = ['sector', 'stock_code', 'stock_name', 'public_date', 'total_share', 
                                      'outstanding_share', 'industry'] #重命名爬取的列

now = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d') #定义昨天的时间

stock_info_sh_name_code_df.to_csv('上证主板A股上市概览%s.csv' % now, encoding='utf-8') #将数据存储至csv中
stock_info_sh_name_code_df1.to_csv('上证科创板上市概览%s.csv' % now, encoding='utf-8') #将数据存储至csv中
stock_info_sz_name_code_df.to_csv('深证A股上市概览%s.csv' % now, encoding='utf-8') #将数据存储至csv中

### 开始爬取历史行情
df = pd.DataFrame()

k = 0

# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol='600000', period='daily', start_date="2020-01-01", end_date=now, adjust="hfq")

# 爬取上证主板A股
for i in range(len(stock_info_sh_name_code_df)):
    # compute the time
    start = time.time()
    
    code = stock_info_sh_name_code_df.loc[i]['stock_code']
    start_date1 =  stock_info_sh_name_code_df.loc[i]['public_date']
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=start_date1, end_date=now, adjust="hfq")
    stock_zh_a_hist_df['stock_code']=code
    df = pd.concat([df, stock_zh_a_hist_df])
    
    end = time.time()
    print(code, 'Running time: %s Seconds'%(end - start))
    
    k = k+1
    print(k, 'SH')
    
    del stock_zh_a_hist_df, code, start_date1, start, end
    
# 爬取上证科创板
for i in range(len(stock_info_sh_name_code_df1)):
    # compute the time
    start = time.time()
    
    code = stock_info_sh_name_code_df.loc[i]['stock_code']
    start_date1 =  stock_info_sh_name_code_df.loc[i]['public_date']
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=start_date1, end_date=now, adjust="hfq")
    stock_zh_a_hist_df['stock_code']=code
    df = pd.concat([df, stock_zh_a_hist_df])
    
    end = time.time()
    print(code, 'Running time: %s Seconds'%(end - start))
    
    k = k+1
    print(k, 'SH')
    
    del stock_zh_a_hist_df, code, start_date1, start, end

# 爬取深证    
for i in range(len(stock_info_sz_name_code_df)):
    # compute the time
    start = time.time()
    
    code = stock_info_sz_name_code_df.loc[i]['stock_code']
    start_date1 =  stock_info_sz_name_code_df.loc[i]['public_date']
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=start_date1, end_date=now, adjust="hfq")
    stock_zh_a_hist_df['stock_code']=code
    df = pd.concat([df, stock_zh_a_hist_df])
    
    end = time.time()
    print(code, 'Running time: %s Seconds'%(end - start))
    
    k = k+1
    print(k, 'SZ')
    
    del stock_zh_a_hist_df, code, start_date1, start, end
    


end1 = time.time()
print('Final_Running time: %s Seconds'%(end1 - start1)) #总花费时间



df.columns = ['date', 'open', 'close', 'highest', 'lowest',
              'VOL', 'turn_volumn', 'amplitude', 'rise_fall', 'rice_fall_value', 'turnover_rate',
              'stock_code'] #重命名爬取的列



# df.to_csv('A_Stock_SH_SZ_Hist.csv', encoding='utf-8') #将数据存储至csv中



#将数据存至MySQL
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



start2 = time.time()
sql = Mysql('localhost', 3306, 'root', '20011226')
# sql.create_db('myfirstdatabase')
sql.insert_DF('myfirstdatabase', df, 'astockhist_sh_sz', method='replace')

# sql.get_data('myfirstdatabase', 'SELECT * FROM astockhist_sh_sz WHERE code = "600000";', '*')

end2 = time.time()
print('Get List_Running time: %s Seconds'%(end2 - start2))









