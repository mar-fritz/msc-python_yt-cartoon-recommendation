# -*- coding: utf-8 -*-
"""
Created on Sat Jan 23 10:12:15 2021

@author: 30694
"""


import nltk
from nltk.corpus import stopwords
from textblob import TextBlob
from textblob import Blobber
from textblob.sentiments import NaiveBayesAnalyzer
from textblob.sentiments import PatternAnalyzer
import numpy as np
import pandas as pd





def remove_stopwords(text):
    STOPWORDS = set(stopwords.words('english'))
    return " ".join([word for word in str(text).split() if word not in STOPWORDS])



def processing(sub_path):
    
    
    df3=pd.read_csv(sub_path)
    df3.columns=['text']
    # convert to lowercase
    df3.text=df3.text.str.lower()
    # remove text inside () or []
    df3.text=df3['text'].str.replace(r"\(.*\)"," ").str.strip()
    df3.text=df3["text"].str.replace(r"(\s*\[.*?\]\s*)", " ").str.strip()
    #remove all other symbols 
    df3.text=df3.text.str.replace('[^A-Za-z0-9 ]+', '')
    #remove stopwords
    df3["text"] = df3["text"].apply(lambda text: remove_stopwords(text))
    # drop empty lines
    df3['text'].replace('', np.nan, inplace=True)
    df3.dropna(subset=['text'], inplace=True)
    df3.reset_index(drop=True,inplace=True)
    # Tokenization and stemming?
    text_to_proc=df3.text.to_list()
    text_to_proc=' '.join(text_to_proc)
    bay_res,pat_res=analyzers(text_to_proc)
    pat_res=(pat_res+1)/2
    result=[bay_res,pat_res]
    return result

def analyzers(text_in):
    bay_res=TextBlob(text_in,analyzer=NaiveBayesAnalyzer())
    pat_res=TextBlob(text_in,analyzer=PatternAnalyzer())
    return bay_res.sentiment.p_pos,pat_res.sentiment.polarity


def analyze(df4):
    nltk.download('stopwords')
    
    df4['analysis_res']=df4.captions.apply(lambda captions:processing(captions)) 
    df4['bayes_res']=df4.analysis_res.apply(lambda x :x[0])
    df4['pattern_res']=df4.analysis_res.apply(lambda x :x[1])
    del df4['analysis_res']
    return df4


