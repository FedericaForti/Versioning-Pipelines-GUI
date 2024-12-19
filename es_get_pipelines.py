import configparser
from elasticsearch import Elasticsearch
from elasticsearch import client
import os
import hashlib
from datetime import datetime

# Directory dove vengono salvate tutte le pipelines
BASE_DIR = "pipelines"


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


#Funzione che serve per calcolare l'hash del contenuto della pipeline (per poi effettuare un confronto con quelle presenti in ES e quelle salvate in locale)
def calculate_hash(content):
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


# Funzione per salvare la pipeline con il controllo delle modifiche (a seguito del confronto di cui sopra), gestisce il versioning
def save_pipeline_with_modification_check(pipeline_id, content):
    
    # Creazione directory specifica per la pipeline
    pipeline_dir = os.path.join(BASE_DIR, pipeline_id)
    os.makedirs(pipeline_dir, exist_ok=True)

    # Percorso per il file più recente (è sempre quello senza data)
    latest_file_path = os.path.join(pipeline_dir, f"{pipeline_id}.conf")

    # Verifica se esiste un file precedente già salvato per la i-sima pipeline
    if os.path.exists(latest_file_path):
        # Legge il contenuto del file già salvato
        with open(latest_file_path, 'r') as file:
            existing_content = file.read()

        # Confronta l'hash dei contenuti (se è uguale non fa nulla, se è diverso significa che ci sono state modifiche: il file precedente viene rinominato con il formato "<id_pipeline>.yyyy.mm.dd-HH.mm.conf")
        if calculate_hash(existing_content) == calculate_hash(content):
            print(f"No changes detected for the pipeline: {pipeline_id}.")
            return
        else:
            # Rinomina il file precedente con il formato "<id_pipeline>.yyyy.mm.dd-HH.mm.conf"
            timestamp = datetime.now().strftime("%Y.%m.%d-%H.%M")
            old_file_name = f"{pipeline_id}.{timestamp}.conf"
            old_file_path = os.path.join(pipeline_dir, old_file_name)
            os.rename(latest_file_path, old_file_path)
            print(f"Pipeline modified. Previous file renamed to: {old_file_name}.")

    # Salva il nuovo file come "<pipeline_id>.conf" (questo è sempre il nome dell'ultima versione rilevata)
    with open(latest_file_path, 'w') as file:
        file.write(content)
    print(f"New pipeline file saved as: {pipeline_id}.conf.")


#Get pipelines + save in local dir
def get_logstash_pipelines(es):
    try:
        pipelines = es.logstash.get_pipeline()
        for pipeline_id, pipeline_data in pipelines.items():
            content = pipeline_data.get('pipeline', 'No definition available')
            save_pipeline_with_modification_check(pipeline_id, content)
    except Exception as e:
        print(f"Error while retrieving Logstash pipelines: {e}")

