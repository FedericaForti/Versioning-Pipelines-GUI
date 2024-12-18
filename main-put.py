# main-put.py
import os
import sys
import glob
from es_put_pipelines import upload_pipeline, connect_elasticsearch


# Main: gestione degli argomenti da terminale
def main():
    if len(sys.argv) < 2:
        print("Uso: python main-put.py <pipeline_id> [file_name] --base_dir <directory_base>")
        sys.exit(1)

    pipeline_id = sys.argv[1]
    file_name = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != "--base_dir" else None
    base_dir = "pipelines/"  #cartella predefinita

    # Estrai il valore di --base_dir
    for i in range(1, len(sys.argv)):
        if sys.argv[i] == "--base_dir":
            if i + 1 < len(sys.argv):
                base_dir = sys.argv[i + 1]
            else:
                print("Errore: --base_dir richiede un argomento.")
                sys.exit(1)

    if not base_dir:
        print("Errore: Devi specificare --base_dir <directory_base>")
        sys.exit(1)

    # Costruisci il percorso della pipeline
    if '.' in pipeline_id:
        pipeline_dir = os.path.join(base_dir, pipeline_id.split('.')[0])
    else:
        pipeline_dir = os.path.join(base_dir, pipeline_id)
    print(f"Directory della pipeline: {pipeline_dir}")

    if file_name:
        # Percorso specifico del file scelto
        target_file_path = os.path.join(pipeline_dir, file_name)
        if not os.path.exists(target_file_path):
            print(f"Errore: Il file '{target_file_path}' non esiste.")
            sys.exit(1)
    else:
        # Cerca un file corrispondente nella directory
        potential_files = glob.glob(os.path.join(pipeline_dir, f"{pipeline_id}.conf"))
        print(f"File trovati in '{pipeline_dir}': {potential_files}")

        if len(potential_files) == 0:
            print(f"Errore: Nessun file trovato per '{pipeline_id}'. Verifica il nome o specifica un file esatto.")
            sys.exit(1)
        elif len(potential_files) > 1:
            print(f"Errore: Sono stati trovati più file per '{pipeline_id}'. Specifica un file esatto.")
            sys.exit(1)
        else:
            target_file_path = potential_files[0]

    # Rimuove la data dal nome della pipeline per Elasticsearch
    if '.' in pipeline_id:
        base_pipeline_id = pipeline_id.split('.')[0]  # Split se contiene un punto
    else:
        base_pipeline_id = pipeline_id  # Usa il nome completo senza split

    # Connetti a Elasticsearch
    es = connect_elasticsearch()
    if es:
        upload_pipeline(es, base_pipeline_id, target_file_path)


if __name__ == "__main__":
    main()
