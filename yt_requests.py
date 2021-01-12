# -*- coding: utf-8 -*-
"""
Created on Thu Dec 31 11:34:04 2020

@author: 30694
"""

import pandas as pd
from googleapiclient.discovery import build
from pytube import YouTube
import isodate


#126,61

def get_videos():
    
    api_key='AIzaSyBDijxihnqUFV-gvZf46z_3t_ZzwntUJjQ'
    youtube= build('youtube', 'v3',developerKey=api_key)
    
    user_input=input("Which cartoon are we watching today?")

# GET VIDEO IDS, TITLES, URLS ,

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
    
    while ('nextPageToken' in response) and (counter<1):
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
        
    # Convert Duration to seconds and add URL
    
    def to_seconds(dur_from_api):
        dur=isodate.parse_duration(str(dur_from_api))
        return dur.total_seconds()
    
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
    
      
    filter_words='compilation|movie|playlist|movies|film'
    data0['v_title']=data0['v_title'].str.lower()
    data2=data0[~data0['v_title'].str.contains(filter_words)]
    data2=data2[data2['v_title'].str.contains(user_input)]
    data2=data2[data2['v_duration']<(4*data2['v_duration'].mean())]
     
    # DOWNNLOAD SUBTITLES
     
    paths=[]
    sub_file_path='C:\\Users\\30694\\OneDrive\\Υπολογιστής\\subs\\'
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
                
    data2['sub_path']=paths
    data2.reset_index(drop=True,inplace=True)
    data2.columns=['id','title','duration_sec','url','captions']
    data2=data2[['id','title','url','duration_sec','captions']]
    
    return(data2)





df1=get_videos()



# def get_likes(video_ids,):
    
#     request4=youtube.videos().list(part='statistics',
#                                    id=req2_ids).execute()
        
#     fields=['id','statistics.viewCount','statistics.likeCount','statistics.dislikeCount']
    
#     temp_df=pd.json_normalize(request4['items'])
#     tempdf2=temp_df[fields]
#     tempdf2.columns=['v_id','v_views','v_likes','v_dislikes']
    
#     return
