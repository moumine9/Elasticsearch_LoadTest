#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################
#                                                           #
#   Updating the elastic search index with fresh datas.     #
#                                                           #
#############################################################

#============================================================
#title           : Load_Test_Search
#description     : Load Testing the search engine
#author          : moumine9
#version         : 1.0
#usage           : python load_test_search.py <inputfile.py>
#notes           :
#python_version  :3.7.0
#============================================================

import os
import re
import sys
import json
import getopt
import requests
import pandas as pd
from faker import Faker
from faker.providers import BaseProvider,person
from elasticsearch import Elasticsearch

import logging
import threading
import time
import random

from multiprocessing import Process, Lock
from multiprocessing.pool import ThreadPool

import argparse

import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

es_host = ""
es_index_name = ""
search_file = ""
oauth_token = ""


parser = argparse.ArgumentParser(description='Description of your program')

def draw_chart(df):

    trace0 = go.Scatter(
        x = df['nbr_threads'],
        y = df['took_mean'],
        mode = 'lines+markers',
        name = 'Took'
    )

    trace1 = go.Scatter(
        x = df['nbr_threads'],
        y = df['elapsed_mean'],
        mode = 'lines+markers',
        name = 'Elapsed'
    )

    trace3 = go.Scatter(
        x = df['nbr_threads'],
        y = df['hits_mean'],
        mode = 'lines+markers',
        name = 'Nbr Hits'
    )


    data = [trace0, trace1]

    plot(data, filename='line-mode')


def search(query):

    query = re.sub(r'\W+', ' ', query)
    
    url = "%s/%s/_search/?q=%s" % ( es_host, es_index_name, query )

    req = requests.get(url, headers =  { 'Authorization': oauth_token })
    #res =  es.search(index=es_index_name, params = {"q":query})

    print(req)

    res = req.json()
    row = [query,res["took"], (req.elapsed.microseconds/1000) , res["hits"]["total"], res["hits"]["max_score"] ]

    return row

def init():

    global es_host, es_index_name, search_file, oauth_token

    parser.add_argument('-eh', '--ehost', default='http://localhost:9200')
    parser.add_argument('-i', '--index', default="index")
    parser.add_argument('-s', '--search', required=True)
    parser.add_argument('-t', '--token')

    args = parser.parse_args()

    es_host = args.ehost
    es_index_name = args.index
    search_file = args.search
    oauth_token = args.token


if __name__ == "__main__":

    init()

    list_queries = []
    results = []
    results_queries = []
    list_index = int(random.uniform(0, len(list_queries) / 2))

    with open(search_file,'r', encoding='utf8' ) as f:
        list_queries = f.readlines()


    for nbr_threads in range(1,101):
        p = ThreadPool(nbr_threads)

        data_to_feed = list_queries[list_index:(list_index+nbr_threads)]

        #data_to_feed = list_queries[0:nbr_threads]

        pool_output = p.map(search, data_to_feed )

        df = pd.DataFrame(data = pool_output, columns = ['query', 'took', 'elapsed' ,'hits', 'max_score'])

        df_means = df.mean( axis = 1, skipna= True )

        #took_mean = (df['took'].sum())/nbr_threads
        #elapsed_mean = df['elapsed'].mean()
        #hits_mean = df['hits'].mean()
        #max_score_mean = df['max_score'].mean()

        results.append( [ df['query'], nbr_threads, df_mean['took'], df_mean['elapsed'], df_mean['hits'], df_mean['max_score'] ] )
        print( "nbr Thread %s" % ( str(nbr_threads) ) )

        p.terminate()
        p.join()


    df = pd.DataFrame(data = results, columns = ['query','nbr_threads', 'took_mean', 'elapsed_mean' ,'hits_mean', 'max_score_mean'])

    draw_chart(df)

    print('Done !')