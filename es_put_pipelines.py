# es_put_pipelines.py
import os
import configparser
from datetime import datetime
from elasticsearch import Elasticsearch


#Read config.ini
def load_config(filename='config.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config


#Connect with Elasticsearch
def connect_elasticsearch():
    config = load_config()
    host = config.get('elasticsearch', 'host')
    user = config.get('elasticsearch', 'user')
    password = config.get('elasticsearch', 'password')
    cert = config.get('elasticsearch', 'cert', fallback=None)

    es = Elasticsearch([host], ca_certs=cert, basic_auth=(user, password))
    if es.ping():
        print("Connected with Elasticsearch")
        return es
    else:
        print("Connection error with Elasticsearch")
        return None


#Put Pipelines 
def upload_pipeline(es, pipeline_id, file_path):
    # Legge il contenuto del file
    with open(file_path, 'r') as file:
        pipeline_content = file.read()

    # Contenuto caricato in ES (body della PUT)
    body = {
        "pipeline": pipeline_content,
        "last_modified": datetime.now().isoformat(timespec='milliseconds') + "Z",
        "pipeline_metadata": {},
        "username": "script_user",
        "pipeline_settings": {}
    }
    try:
        # Put Pipeline
        es.logstash.put_pipeline(id=pipeline_id, body=body)
        print(f"Pipeline '{pipeline_id}' successfully loaded into Elasticsearch.")
    except Exception as e:
        print(f"Error while loading the pipeline'{pipeline_id}': {e}")
