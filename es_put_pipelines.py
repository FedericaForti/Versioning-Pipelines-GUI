# es_put_pipelines.py
import os
import configparser
from datetime import datetime
from elasticsearch import Elasticsearch


# Leggi il file di configurazione
def load_config(filename='config.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config


# Connessione a Elasticsearch
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


# Funzione per caricare una pipeline in Elasticsearch
def upload_pipeline(es, pipeline_id, file_path):
    """Carica una pipeline in Elasticsearch con il nome pipeline_id."""
    # Legge il contenuto del file
    with open(file_path, 'r') as file:
        pipeline_content = file.read()

    # Corpo della richiesta
    body = {
        "pipeline": pipeline_content,
        "last_modified": datetime.now().isoformat(timespec='milliseconds') + "Z",
        "pipeline_metadata": {},
        "username": "script_user",
        "pipeline_settings": {}
    }
    try:
        # Effettua il PUT della pipeline su Elasticsearch
        es.logstash.put_pipeline(id=pipeline_id, body=body)
        print(f"Pipeline '{pipeline_id}' caricata con successo in Elasticsearch.")
    except Exception as e:
        print(f"Errore durante il caricamento della pipeline '{pipeline_id}': {e}")
