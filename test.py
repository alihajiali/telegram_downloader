from concurrent.futures import ThreadPoolExecutor 
from elasticsearch import Elasticsearch, helpers
import datetime
import requests
from tqdm import tqdm
import time
import os
es = Elasticsearch("http://192.168.1.45:9200/")
telegram_elastic = Elasticsearch("http://elastic:Twitter_Crawler@89.163.129.60:9200/")

es.delete_by_query(index="telegram_messages_2", body={"query":{"match_all":{}}})


#### transfer
telegrams = es.search(index="telegram_messages")["hits"]["hits"]
for telegram in telegrams:
    es.index(index="telegram_messages_2", id=telegram["_id"], document=telegram["_source"])




#### remove nginx

def x(file):
    requests.get(f"http://89.163.129.60:9000/del_file/{file}", auth=('user', 'Telegram_Cr@wler'))

lst = requests.get(f"http://89.163.129.60:9000/list_files/", auth=('user', 'Telegram_Cr@wler')).json()

with ThreadPoolExecutor(max_workers=20) as executor:
    future = executor.map(x, lst)



#### empty elastic

telegram_elastic.delete_by_query(index="telegram_media_download", body={"query":{"match_all":{}}})