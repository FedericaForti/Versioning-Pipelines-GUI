import configparser
from elasticsearch import Elasticsearch
from elasticsearch import client
import os
import hashlib
from datetime import datetime


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

    es = Elasticsearch([host], ca_certs=cert,  http_auth=(user, password))

    if es.ping():
        print("Connected with Elasticsearch")
        return es
    else:
        print("Connection error with Elasticsearch")
        return None


#Get cluster health
def get_cluster_health(es):

    health = es.cluster.health()
    print("Cluster Health:", health['status'] )


# Directory dove vengono salvate tutte le pipelines
BASE_DIR = "pipelines"

# Calcola hash per confronto con pipeline già salvata
def calculate_hash(content):
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

# Salva la pipeline in una directory per pipeline ID
def save_pipeline_in_directory(pipeline_id, content):
    # Crea la directory specifica per la pipeline
    pipeline_dir = os.path.join(BASE_DIR, pipeline_id)
    os.makedirs(pipeline_dir, exist_ok=True)
    
    # Nome del file 
    #today_date = datetime.now().strftime("%Y-%m-%d")
    #file_name = f"{today_date}-{pipeline_id}.conf"
    file_name = f"{pipeline_id}.conf"
    file_path = os.path.join(pipeline_dir, file_name)
    
    # Verifica se esiste un file con lo stesso contenuto
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            existing_content = file.read()
            if calculate_hash(existing_content) == calculate_hash(content):
                print(f"Pipeline {pipeline_id} già salvata come {file_name}. Nessuna modifica.")
                return
            else:
                print(f"Pipeline {pipeline_id} modificata. Sovrascrivendo {file_name}.")
    
    # Salva la pipeline
    with open(file_path, 'w') as file:
        file.write(content)
    print(f"Pipeline {pipeline_id} salvata in {file_path}")

# Get Pipelines
def get_logstash_pipelines(es):
    try:
        pipelines = es.logstash.get_pipeline()
        for pipeline_id, pipeline_data in pipelines.items():
            content = pipeline_data.get('pipeline', 'No definition available')

            # Salva la pipeline nella directory specifica
            save_pipeline_in_directory(pipeline_id, content)
    
    except Exception as e:
        print(f"Errore durante il recupero delle pipeline Logstash: {e}")