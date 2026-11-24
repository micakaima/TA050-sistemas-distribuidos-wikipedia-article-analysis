import os
import kagglehub
import argparse
import shutil
import json

KAGGLE_DATASET_BASE_PATH = "wikimedia-foundation/wikipedia-structured-contents"
KAGGLE_EN_FILE_BASE_PATH = "enwiki/data/enwiki_namespace_0_"
KAGGLE_FR_FILE_BASE_PATH = "frwiki/data/frwiki_namespace_0_"
DEFAULT_INPUT_BASE_PATH = "./data/input"

DEFAULT_NUM_CLIENTS=3
DEFAULT_EN_FILES_SAMPLE_AMOUNT=1
DEFAULT_FR_FILES_SAMPLE_AMOUNT=1

CONFIG_FILE_PATH = "./scripts/configs/generate_different_inputs_config.json"

def load_config(config_path=CONFIG_FILE_PATH):
    if not os.path.exists(config_path):
        raise FileNotFoundError("Config file not found")

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    paths_cfg = data.get("paths", {})
    gen_cfg = data.get("generation", {})

    return {
        "input_base_path": paths_cfg.get("input_base_path", DEFAULT_INPUT_BASE_PATH),
        "num_clients": int(gen_cfg.get("num_clients", DEFAULT_NUM_CLIENTS)),
        "en_files_sample_amount": int(gen_cfg.get("en_files_sample_amount", DEFAULT_EN_FILES_SAMPLE_AMOUNT)),
        "fr_files_sample_amount": int(gen_cfg.get("fr_files_sample_amount", DEFAULT_FR_FILES_SAMPLE_AMOUNT)),
    }

# Permite generar DIFERENTES archivos parquet de input para todos los clientes.
#   - Recibe como argumento '-en' la cantidad de archivos parquet en ingles a descargar.
#   - Recibe como argumento '-fr' la cantidad de archivos parquet en frances a descargar.
#   - Recibe como argumento '-c' el 'ID' del cliente/dataset (ej. '-c 0' genera el directorio input/input_0).
#   - Si no se recibe alguno de estos argumentos, el valor default es siempre 1.
# NOTA: Al ejecutar este script, se borran todos los inputs que habian en data/input.
def parse_arguments(config):
    parser = argparse.ArgumentParser(description="Distributor of datasets between clients")
    parser.add_argument("-en", "--en", type=int, default=config["en_files_sample_amount"], help="English files sample amount")
    parser.add_argument("-fr", "--fr", type=int, default=config["fr_files_sample_amount"], help="French files sample amount")
    parser.add_argument("-c", "--client", type=int, default=None, help="Client ID")
    return parser.parse_args()

def clean_old_inputs(config):
    if os.path.exists(config["input_base_path"]):
        shutil.rmtree(config["input_base_path"])
    os.makedirs(config["input_base_path"])

def create_client_dir(client_id, config):
    client_dir_name = os.path.join(config["input_base_path"], f"input_{client_id}")
    os.makedirs(client_dir_name, exist_ok=True)
    return client_dir_name

def download_and_distribute(file_base_path, sample_amount, client_dirs):
    idx = 0
    for _, target_dir in enumerate(client_dirs):
        for _ in range(sample_amount):
            file_name = f"{file_base_path}{idx:05}.parquet"
            try:
                dataset_path = kagglehub.dataset_download(KAGGLE_DATASET_BASE_PATH, path=file_name)
                output_filename = os.path.join(target_dir, os.path.basename(file_name))
                os.replace(dataset_path, output_filename)
            except Exception as e:
                print(f"[ERROR] While processing {file_name}: {e}")
            idx += 1

def main():
    config = load_config()
    clean_old_inputs(config)
    args = parse_arguments(config)

    client_dirs = []
    if args.client is not None:
        client_dir_name = create_client_dir(args.client, config)
        client_dirs.append(client_dir_name)
    else:
        for c in range(config["num_clients"]):
            client_dir_name = create_client_dir(c, config)
            client_dirs.append(client_dir_name)

    download_and_distribute(KAGGLE_EN_FILE_BASE_PATH, args.en, client_dirs)
    download_and_distribute(KAGGLE_FR_FILE_BASE_PATH, args.fr, client_dirs)

if __name__ == "__main__":
    main()