from elasticsearch import Elasticsearch, helpers
import time

es = Elasticsearch("http://192.168.1.45:9200/")
telegram_elastic = Elasticsearch("http://elastic:Twitter_Crawler@89.163.129.60:9200/")
telegram_index = "telegram_messages"

def add_to_download_saf():
    if telegram_elastic.count(index="telegram_media_download")["count"] <= 1000:
        query = {"bool":{"must_not":[
            {"exists": {"field": "sahab_metadata.media.local_url"}}, 
            {"exists":{"field":"sahab_metadata.media.download_saf"}}
        ], "must":[
            {"exists":{"field":"channel_name"}}, 
            {"exists":{"field":"id"}}
        ]}}
        data = es.search(index=telegram_index, size=100, query=query, sort=[{"sahab_metadata.timestamp":{"order":"desc"}}])["hits"]["hits"]
        lst = []
        for item in data:
            es.update(index=telegram_index, id=item["_id"], doc={"sahab_metadata":{"media":[{"download_saf":True}]}})
            lst.append({
                "_index":"telegram_media_download", 
                "_id":item["_id"], 
                "_source":{
                    "channel_id": item["_source"]["channel_name"],
                    "message_id": item["_source"]["id"],
                    "download_date": None,
                    "download_status": None,
                    "file_type": None,
                    "file_name": None
                }
            })
        helpers.bulk(telegram_elastic, lst)
        print("=============== added ===============")
