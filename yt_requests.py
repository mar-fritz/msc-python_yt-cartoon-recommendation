# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 11:34:04 2020

@author: 30694
"""

import pandas as pd
from googleapiclient.discovery import build
from pytube import YouTube
import isodate
from datetime import datetime
import time



api_key='AIzaSyBDijxihnqUFV-gvZf46z_3t_ZzwntUJjQ'
youtube= build('youtube', 'v3',developerKey=api_key)
sub_file_path='C:\\Users\\30694\\OneDrive\\Υπολογιστής\\subs\\' # insert file path for downloading subtitles

def get_videos():
    
    
    
    user_input=input("Which cartoon are we watching today?")

# GET VIDEO IDS, TITLES, URLS ,

    print('Getting Videos.')
    request_videos = youtube.search().list(
            part="snippet",
            maxResults=50,
            q=user_input + " cartoon english subtitles",
            type='video',
            videoCaption='closedCaption'
        )
    response = request_videos.execute()
    
    
    fields_video=['id.videoId','snippet.title']
    
    tempdf0=pd.json_normalize(response['items'])
    
    data0=tempdf0[fields_video]
    data0.columns=['v_id','v_title']
    next_page_token=response.get('nextPageToken')
    
    video_ids=data0['v_id'].tolist()
    req2_ids=','.join(video_ids)
    request2 = youtube.videos().list(                              
                                    part='contentDetails',
                                    id=req2_ids                               
                                    )
    
    response2=request2.execute()
    fields=['id','contentDetails.duration']
    tempdf1=pd.json_normalize(response2['items'])
    data_dur=tempdf1[fields]
    data_dur.columns=['v_id','v_duration']
    data0=pd.merge(data0,data_dur)
    
    counter=0
    
    # loop for next page ( define count for number of pages)
    
    while ('nextPageToken' in response) and (counter<10): # Each request can fetch up to 50 video information
        response=youtube.search().list(
            part="snippet",
            maxResults=50,
            q=user_input + " cartoon english subtitles",
            type='video',
            videoCaption='closedCaption',
            pageToken=next_page_token
        ).execute()
        
        tempdf0=pd.json_normalize(response['items'])
        data_temp=tempdf0[fields_video]
        data_temp.columns=['v_id','v_title']
        
                
        # GET DURATION
        
        
        video_ids=data_temp['v_id'].tolist()
        req2_ids=','.join(video_ids)
        request2 = youtube.videos().list(                              
                                    part='contentDetails',
                                    id=req2_ids                               
                                    )
    
        response2=request2.execute()
        fields=['id','contentDetails.duration']
        tempdf1=pd.json_normalize(response2['items'])
        data_dur=tempdf1[fields]
        data_dur.columns=['v_id','v_duration']
        data1=pd.merge(data_temp,data_dur)
        
        data0=data0.append(data1,ignore_index=True)
        counter+=1    
    print('Pulled a total of '+str(len(data0))+' videos.')
    # Convert Duration to seconds and add URL
    
    def to_seconds(dur_from_api):
        dur=isodate.parse_duration(str(dur_from_api))
        return dur.total_seconds()
    
    print('Converting duration to seconds.')
    data0['v_duration']=data0['v_duration'].apply(to_seconds)
    data0['v_url']='https://www.youtube.com/watch?v='+data0['v_id'].astype(str)
    
    # GET 3 PLAYLISTS
    
    # request_playlists=youtube.search().list(
    #         part="snippet",
    #         maxResults=3,
    #         q=user_input + " cartoon english subtitles",
    #         type='playlist',
    #     )
    
    # response_playlists=request_playlists.execute()
    # tempdf0=pd.json_normalize(response_playlists['items'])
    # playlists=tempdf0['id.playlistId']
    
    print('Filtering videos.')
    filter_words='compilation|movie|playlist|movies|film'
    data0['v_title']=data0['v_title'].str.lower()
    data2=data0[~data0['v_title'].str.contains(filter_words)]
    data2=data2[data2['v_title'].str.contains(user_input)]
    data2=data2[data2['v_duration']<(4*data2['v_duration'].mean())]
    data2.drop_duplicates(subset=['v_id'],inplace=True)
    print(str(len(data2))+' videos remaining after filtering.')
    # DOWNNLOAD SUBTITLES
    
    print('Downloading subtitles.')
    no_sub=0
    paths=[]
    
    for i in data2['v_id']:
    
        ytsource=YouTube('https://www.youtube.com/watch?v='+i)
        
        if 'en' in ytsource.captions:
            en_caption=ytsource.captions['en']
            en_caption_convert_srt=(en_caption.generate_srt_captions())
            text_file=open(sub_file_path+'subs_'+i+'.txt','w',encoding='utf-8')
            text_file.write(en_caption_convert_srt)
            text_file.close()
            paths.append(sub_file_path+'subs_'+i+'.txt')
            
        else:
            indexNames=data2[data2['v_id']==i].index
            data2.drop(indexNames,inplace=True)
            no_sub+=1
    
    print('Dropped '+str(no_sub)+' videos because no english subtitles were found.')
    data2['sub_path']=paths
    
    data2.reset_index(drop=True,inplace=True)
    data2.columns=['id','title','duration_sec','url','captions']
    data2=data2[['id','title','url','duration_sec','captions']]
    
    print('Downloaded a total of '+str(len(data2))+' subtitles and saved them to '+sub_file_path+'.')
    print('Get_videos completed.')
    return(data2)





def get_views_likes_dislikes(df_in1):
      
      timestamp=datetime.now()
      chunk_size=50
      list_df=[df_in1.id[i:i+chunk_size] for i in range(0,df_in1.shape[0],chunk_size)]
      tempdf3=pd.DataFrame()
      for i in list_df:
          
          id_list=i.tolist()
          request_ids=','.join(id_list)
      
          request=youtube.videos().list(part='statistics',
                                    id=request_ids).execute()
        
          fields=['id','statistics.viewCount','statistics.likeCount','statistics.dislikeCount']
          temp_df=pd.json_normalize(request['items'])
          tempdf2=temp_df[fields]
          tempdf2.columns=['video_id','view_count','like_count','dislike_count']
          print(tempdf2)
          tempdf3=tempdf3.append(tempdf2)
          tempdf3.reset_index(drop=True,inplace=True)
          tempdf3['date_time']=timestamp
          tempdf3=tempdf3[['video_id','date_time','view_count','like_count','dislike_count']]
      return(tempdf3)

def get_statistics(df_in):
    time_interval=input('Specify sample frequency in seconds ')
    counter=0
    while counter<48:
        print('Getting statistics, .')
        df_out=get_views_likes_dislikes(df_in)
        #
        #insert df_out into database!!!
        #
        #
        print('Waiting '+time_interval+' seconds! '+ 'tik-tok..')
        time.sleep(int(time_interval))
        counter+=counter+1
    print('Finished!')

df1=get_videos()
df2=get_statistics(df1)

