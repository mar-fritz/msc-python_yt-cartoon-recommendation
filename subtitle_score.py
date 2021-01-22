# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 19:02:34 2021

@author: 30694
"""

import pandas as pd
import re
# from yt_requests import get_videos


def convert_duration(timein):
    duration=(int(timein[17:19])*3600+int(timein[20:22])*60+int(timein[23:25])+int(timein[26:29])/1000
              -int(timein[0:2])*3600-int(timein[3:5])*60-int(timein[6:8])-int(timein[9:12])/1000)
    return duration

def sub_score(row):
    if re.match('^\w',row['sub_text']):
        return row['sub_dur']
    else:
        return 0

def get_sub_score(path):
   
    with open(path,encoding='utf-8') as f:
        df1 = [line for line in f.read().splitlines() if line]
    df=pd.DataFrame({'col1':df1})
    dur_list=df.iloc[1::3,0].to_list()
    text_list=df.iloc[2::3,0].to_list()
    df=pd.DataFrame({'sub_text':text_list,
                    'sub_dur':dur_list})
    
    df['sub_dur']=df['sub_dur'].apply(convert_duration)
    
    df['captions_score']=df.apply(lambda row: sub_score(row),axis=1)
    final_score=(df.sub_score.sum())/(df.sub_dur.sum())
    open(path,'w').close()
    df.sub_text.to_csv(path,index=False,header=False,encoding='utf-8')
    return final_score

def subtitle_scoring(df1):
     print('subtitle_score Started')
     df1['captions_score']=df1.apply(lambda row: get_sub_score(row['captions']),axis=1)
     print('Subtitle rating completed')
     len1=len(df1)
     df1.drop(df1[(df1.captions_score<df1.captions_score.quantile(.2))].index,inplace=True)
     len2=str(len(df1)-len1)
     print('Dropped '+len2+' subtitles ( below 20% percentile)')
     df1.reset_index(drop=True,inplace=True)
     ###TODO delete droped sub files
     return df1
    
#%%
# if __name__ == '__main__':
#     dfout=subtitle_scoring(dfin)




#%%

# FOR TESTING PURPOSES

# api_k = 'AIzaSyBDijxihnqUFV-gvZf46z_3t_ZzwntUJjQ'
# file_path = 'C:\\Users\\ProAdmin\\Desktop\\Big_Data\\python_sub_file\\'
# df1=get_videos(api_k,file_path)





