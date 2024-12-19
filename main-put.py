# python main-put.py <pipeline_id> --base_dir /path/to/pipelines
import os
import sys
import glob
from es_put_pipelines import upload_pipeline, connect_elasticsearch



def main():
    #Gestione degli argomenti da terminale e costruzione di: base_pipeline_id
    #Se non è specificata la pipeline_id
    if len(sys.argv) <=1:
        print("You must specify: <pipeline_id> \n\nUse: python main-put.py <pipeline_id>")
        sys.exit(1)

    pipeline_id = sys.argv[1]
    file_name = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != "--base_dir" else None
    base_dir = "pipelines/"  #cartella predefinita

    # Estrazione valore --base_dir
    for i in range(1, len(sys.argv)):
        if sys.argv[i] == "--base_dir":
            if i + 1 < len(sys.argv):
                base_dir = sys.argv[i + 1]
            else:
                print("Error: --base_dir require at least an argument.")
                sys.exit(1)

    # Costruizione percorso della pipeline per la ricerca del file
    if '.' in pipeline_id:
        pipeline_dir = os.path.join(base_dir, pipeline_id.split('.')[0])
    else:
        pipeline_dir = os.path.join(base_dir, pipeline_id)
    print(f"Pipeline directory: {pipeline_dir}")

    if file_name:
        # Percorso specifico del file scelto
        target_file_path = os.path.join(pipeline_dir, file_name)
        if not os.path.exists(target_file_path):
            print(f"Error: file '{target_file_path}' does not exist.")
            sys.exit(1)
    else:
        # Cerca UN file corrispondente nella directory
        potential_files = glob.glob(os.path.join(pipeline_dir, f"{pipeline_id}.conf"))
        print(f"Files found in '{pipeline_dir}': {potential_files}")

        if len(potential_files) == 0:
            print(f"Error: No file found for '{pipeline_id}'. Verify the name or specify an exact file.")
            sys.exit(1)
        elif len(potential_files) > 1:
            print(f"Error: Multiple files found for '{pipeline_id}'. Specify an exact file.")
            sys.exit(1)
        else:
            target_file_path = potential_files[0]

    # Rimuove la data dal nome della pipeline per Elasticsearch se la contiene, altrimenti utilizza il nome esatto
    if '.' in pipeline_id:
        base_pipeline_id = pipeline_id.split('.')[0]  # Split se contiene un punto (il punto non può essere scelto come pipeline_id)
    else:
        base_pipeline_id = pipeline_id  

    # Connect to Elasticsearch
    es = connect_elasticsearch()
    if es:
        upload_pipeline(es, base_pipeline_id, target_file_path)


if __name__ == "__main__":
    main()
