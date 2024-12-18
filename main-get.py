import os
from es_get_pipelines import connect_elasticsearch, get_cluster_health, get_logstash_pipelines

def main():
    os.makedirs("pipelines", exist_ok=True)  # Assicura che la directory per le pipeline esista
    
    es = connect_elasticsearch()

    if es:
        get_cluster_health(es)
        get_logstash_pipelines(es)
    else:
        print("Error! It's not possible to connect with Elasticsearch")

if __name__ == "__main__":
    main()
