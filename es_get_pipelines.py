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

def calculate_hash(content):
    """Calcola l'hash SHA256 di una stringa."""
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

import os
import hashlib
from datetime import datetime

# Directory base per le pipeline
BASE_DIR = "pipelines"

# Funzione per calcolare l'hash di un contenuto
def calculate_hash(content):
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

# Funzione per salvare la pipeline con il controllo delle modifiche
def save_pipeline_with_modification_check(pipeline_id, content):
    """Salva la pipeline, gestendo il versioning."""
    # Directory specifica per la pipeline
    pipeline_dir = os.path.join(BASE_DIR, pipeline_id)
    os.makedirs(pipeline_dir, exist_ok=True)

    # Percorso per il file più recente
    latest_file_path = os.path.join(pipeline_dir, f"{pipeline_id}.conf")

    # Verifica se esiste un file precedente
    if os.path.exists(latest_file_path):
        # Legge il contenuto del file corrente
        with open(latest_file_path, 'r') as file:
            existing_content = file.read()

        # Confronta l'hash dei contenuti
        if calculate_hash(existing_content) == calculate_hash(content):
            print(f"Nessuna modifica rilevata per la pipeline {pipeline_id}.")
            return
        else:
            # Rinomina il file precedente con il formato "<id_pipeline>-yyyy.mm.dd-HH.mm.conf"
            timestamp = datetime.now().strftime("%Y.%m.%d-%H.%M")
            old_file_name = f"{pipeline_id}.{timestamp}.conf"
            old_file_path = os.path.join(pipeline_dir, old_file_name)
            os.rename(latest_file_path, old_file_path)
            print(f"Pipeline modificata. File precedente rinominato in {old_file_name}.")

    # Salva il nuovo file come "<pipeline_id>.conf"
    with open(latest_file_path, 'w') as file:
        file.write(content)
    print(f"Nuovo file pipeline salvato come {pipeline_id}.conf.")


def get_logstash_pipelines(es):
    """Recupera le pipeline da Elasticsearch e salva con gestione modifiche."""
    try:
        pipelines = es.logstash.get_pipeline()
        for pipeline_id, pipeline_data in pipelines.items():
            content = pipeline_data.get('pipeline', 'No definition available')
            save_pipeline_with_modification_check(pipeline_id, content)
    except Exception as e:
        print(f"Errore durante il recupero delle pipeline Logstash: {e}")

