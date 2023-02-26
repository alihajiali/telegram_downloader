from elasticsearch import Elasticsearch
import datetime
import requests
import time
import os
es = Elasticsearch("http://192.168.1.45:9200/")
telegram_elastic = Elasticsearch("http://elastic:Twitter_Crawler@89.163.129.60:9200/")
telegram_index = "telegram_messages"
cdn_address = "http://aiengines.ir:8101/telegram/"

def get(medias):
    list_channels = os.listdir("./files/")
    for media in medias:
        media_id = media["_id"]
        channel_id = media["_source"]["channel_id"]
        message_id = media["_source"]["message_id"]
        download_date = media["_source"]["download_date"]
        file_type = media["_source"]["file_type"]
        file_name = media["_source"]["file_name"]

        if file_name != "not_specified":
            if channel_id not in list_channels:
                os.mkdir(f"./files/{str(channel_id)}")
            list_extentions = os.listdir(f"./files/{str(channel_id)}")
            list_channels.append(str(channel_id))

            if file_type not in list_extentions:
                os.mkdir(f"./files/{str(channel_id)}/{str(file_type)}")
            try:
                response = requests.get("http://89.163.129.60/downloads/"+file_name)
                print(response.status_code)
                if response.status_code == 200:
                    file_address = f"files/{str(channel_id)}/{str(file_type)}/{str(file_name)}"
                    with open(file_address, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=1024 * 8):
                            if chunk:
                                f.write(chunk)
                                f.flush()
                                os.fsync(f.fileno())
                    print("")
                telegram_elastic.delete(index="telegram_media_download", id=media_id)
                result = requests.get(f"http://89.163.129.60:9000/del_file/{file_name}", auth=('user', 'Telegram_Cr@wler'))
                print(result.status_code, result.json())
                result = es.update(index=telegram_index, id=media_id, doc={"sahab_metadata":{"media":[{"local_url":cdn_address+file_address}]}})
                print(result)
            except:pass
        else:
            telegram_elastic.delete(index="telegram_media_download", id=media_id)

def downloader():
    query = {"bool":{"must":[
        {"exists":{"field":"download_status"}}
    ]}}
    if telegram_elastic.count(index="telegram_media_download", body={"query":query})["count"] > 0:
        medias = telegram_elastic.search(index="telegram_media_download", size=1000, query=query)["hits"]["hits"]
        get(medias)
    time.sleep(1)
