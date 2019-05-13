#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################
#                                                           #
#   Updating the elastic search index with fresh datas.     #
#                                                           #
#############################################################

#============================================================
#title           :filling_index.py
#description     : Filling the elastic search index.
#author          :maha
#date            :2019-04-12
#version         :1.0
#usage           :python filling_index.py <inputfile.json>
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

es_host = 'http://localhost'
es_port = 9200
es_index_name = 'customers'

es = Elasticsearch([{'host': es_host, 'port': es_port}])

fake = Faker()

df_all = pd.DataFrame(data = [], columns = ['Session','Elapsed','Status', 'ContentSize'])

def generate_fake_data(fake):
    
    indexRow = {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "address":fake.street_address(),
        "phone_number":fake.phone_number(),
        "email":fake.email(),
        "job": fake.job(),
        "company":fake.company(),
        "biography":fake.paragraph(nb_sentences=5, variable_nb_sentences=True, ext_word_list=None)
    }

    return indexRow

def client_insert(fake,number_hits):

    global df_all
    
    results = []

    url = "%s:%s/%s/_doc" % ( es_host, es_port, es_index_name )
    session_name = fake.city()

    for i in range(0, number_hits):

        fake_data = json.dumps(generate_fake_data(fake))

        req = requests.post(url, data= fake_data, headers =  {'content-type': 'application/json'})

        elapsed = (req.elapsed.microseconds/1000)
        status = req.status_code
        content_size = len( fake_data )

        row = [ session_name, elapsed, status, content_size ]

        results.append(row)
    
    df = pd.DataFrame(data = results, columns = ['Session','Elapsed','Status', 'ContentSize'])

    df_all = df_all.append(df, ignore_index=True)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    threads = list()
    for index in range(50):
        logging.info("Main    : create and start thread %d.", index)
        x = threading.Thread(target=client_insert, args=(fake, int(random.uniform(1000000,2000000)) ))
        threads.append(x)
        x.start()

    """     for index, thread in enumerate(threads):
            logging.info("Main    : before joining thread %d.", index)
            thread.join()
            logging.info("Main    : thread %d done", index) """

    df_all.to_csv('results_ldtests.csv', index = False)

