# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 12:54:21 2023

@author: C.Z.J
"""
from __future__ import (absolute_import, division, print_function, 
                        unicode_literals)

import datetime # For datetime objects
import os.path # To manage paths
import sys # To find out the script name (in argv[0])



# Import the backtrader platform
import backtrader as bt

# Import needed packages
import pandas as pd
import matplotlib.pyplot as plt



# MySql
import MySQLdb as MS
from sqlalchemy import create_engine
import pymysql
import pandas as pd
from pandas._libs import Timestamp



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



# Create a Strategy
class TestStrategy(bt.Strategy):
    params = (
        ('maperiod', 15),
    )

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # Add a MovingAverageSimple indicator
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod)

        # Indicators for the plotting show
        bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        bt.indicators.WeightedMovingAverage(self.datas[0], period=25,
                                            subplot=True)
        bt.indicators.StochasticSlow(self.datas[0])
        bt.indicators.MACDHisto(self.datas[0])
        rsi = bt.indicators.RSI(self.datas[0])
        bt.indicators.SmoothedMovingAverage(rsi, period=10)
        bt.indicators.ATR(self.datas[0], plot=False)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            # Not yet ... we MIGHT BUY if ...
            if self.dataclose[0] > self.sma[0]:

                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataclose[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()

        else:

            if self.dataclose[0] < self.sma[0]:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.dataclose[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()


if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    datapath = os.path.join(modpath, '../../datas/orcl-1995-2014.txt')

    # Create a Data Feed
    data = bt.feeds.YahooFinanceCSVData(
        dataname=datapath,
        # Do not pass values before this date
        fromdate=datetime.datetime(2000, 1, 1),
        # Do not pass values before this date
        todate=datetime.datetime(2000, 12, 31),
        # Do not pass values after this date
        reverse=False)

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)

    # Set our desired cash start
    cerebro.broker.setcash(1000.0)

    # Add a FixedSize sizer according to the stake
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)

    # Set the commission
    cerebro.broker.setcommission(commission=0.0)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Plot the result
    cerebro.plot(style='seaborn')
