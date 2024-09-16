# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 15:53:04 2023

@author: C.Z.J
"""
import pandas as pd
import time
import akshare as ak
import datetime


# compute the time
start1 = time.time()


### get data of today Shanghai, Shenzhen type A stock info
start = time.time()
stock_info_sh_name_code_df = ak.stock_info_sh_name_code(symbol='主板A股')
stock_info_sh_name_code_df1 = ak.stock_info_sh_name_code(symbol='科创板')
# print(stock_info_sh_name_code_df)
stock_info_sz_name_code_df = ak.stock_info_sz_name_code(indicator='A股列表')
# print(stock_info_sz_name_code_df)

end = time.time()
print('Get List_Running time: %s Seconds'%(end - start))

del start, end


### get time data
now = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d') # yesterday
# now = now_time.strftime('%Y-%m-%d') # today


### judge if today market stock change or not, not change: import today data; change: update database
df = pd.DataFrame()

k = 0

# get A stock Shanghai
for i in range(len(stock_info_sh_name_code_df)):
    # compute the time
    start = time.time()
    
    code = stock_info_sh_name_code_df.loc[i]['证券代码']
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=now, end_date=now, adjust="hfq")
    stock_zh_a_hist_df['code']=code
    df = pd.concat([df, stock_zh_a_hist_df])
    
    end = time.time()
    print(code, 'Running time: %s Seconds'%(end - start))
    
    k = k+1
    print(k, 'SH')
    
    del stock_zh_a_hist_df, code, start, end
    
# get A stock Shenzhen 
for i in range(len(stock_info_sz_name_code_df)):
    # compute the time
    start = time.time()
    
    code = stock_info_sz_name_code_df.loc[i]['A股代码']
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period='daily', start_date=now, end_date=now, adjust="hfq")
    stock_zh_a_hist_df['code']=code
    df = pd.concat([df, stock_zh_a_hist_df])
    
    end = time.time()
    print(code, 'Running time: %s Seconds'%(end - start))
    
    k = k+1
    print(k, 'SZ')
    
    del stock_zh_a_hist_df, code, start, end
    


end1 = time.time()
print('Final_Running time: %s Seconds'%(end1 - start1)) # Total spending time

df.columns = ['date', 'open', 'close', 'highest', 'lowest',
              'VOL', 'turn_volumn', 'amplitude', 'rise_fall', 'rice_fall_value', 'turnover_rate',
              'stock_code'] # rename columns
    

    


