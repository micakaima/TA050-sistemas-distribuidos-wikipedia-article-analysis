import os
import kagglehub
import argparse
import shutil
import pyarrow.parquet as pq

KAGGLE_DATASET_BASE_PATH = "wikimedia-foundation/wikipedia-structured-contents"
KAGGLE_EN_FILE_BASE_PATH = "enwiki/data/enwiki_namespace_0_"
KAGGLE_FR_FILE_BASE_PATH = "frwiki/data/frwiki_namespace_0_"
INPUT_BASE_PATH = "./data/input"

NUM_CLIENTS=3
MAX_ROWS=30

# Permite generar archivos parquet de input truncados para un cliente/s.
#   - Recibe como argumento '-c' el 'ID' del cliente/dataset (ej. '-c 0' genera el directorio input/input_0).
#   - Recibe como argumento '-max' el maximo de filas que debe contener el archivo parquet.
#   - Si no recibe el argumento '-c', por default genera inputs truncados para 3 clientes.
#   - Si no reicbe el argumento '-max', por default deja hasta 30 filas en cada archivo parquet.
# NOTA: Al ejecutar este script, se borran todos los inputs que habian en data/input.
def parse_arguments():
    parser = argparse.ArgumentParser(description="Generator of short datasets")
    parser.add_argument("-c", "--client", type=int, default=None, help="Client ID")
    parser.add_argument("-max", "--max_rows", type=int, default=None, help="Max Rows")
    return parser.parse_args()

def clean_old_inputs():
    if os.path.exists(INPUT_BASE_PATH):
        shutil.rmtree(INPUT_BASE_PATH)
    os.makedirs(INPUT_BASE_PATH)

def create_client_dir(client_id):
    client_dir_name = os.path.join(INPUT_BASE_PATH, f"input_{client_id}")
    os.makedirs(client_dir_name, exist_ok=True)
    return client_dir_name

def download_and_distribute(file_base_path, client_dirs, lang, max_rows):
    idx = 0
    for _, target_dir in enumerate(client_dirs):
        file_name = f"{file_base_path}{idx:05}.parquet"
        try:
            dataset_path = kagglehub.dataset_download(KAGGLE_DATASET_BASE_PATH, path=file_name)
            output_filename = os.path.join(target_dir, f"{lang}-truncated.parquet")
            truncate_parquet_file(dataset_path, output_filename, max_rows)
        except Exception as e:
            print(f"[ERROR] While processing {file_name}: {e}")
        idx += 1

def truncate_parquet_file(file, output_path, max_rows):
    parquet_file = pq.ParquetFile(file)
    schema = parquet_file.schema.to_arrow_schema()

    rows_written = 0
    with pq.ParquetWriter(output_path, schema) as writer:
        for i in range(parquet_file.num_row_groups):
            if rows_written >= max_rows:
                break
            table = parquet_file.read_row_group(i)
            remaining = max_rows - rows_written
            if table.num_rows > remaining:
                table = table.slice(0, remaining)
            writer.write_table(table)
            rows_written += table.num_rows


def main():
    clean_old_inputs()
    args = parse_arguments()

    client_dirs = []
    if args.client is not None:
        client_dir_name = create_client_dir(args.client)
        client_dirs.append(client_dir_name)
    else:
        for c in range(NUM_CLIENTS):
            client_dir_name = create_client_dir(c)
            client_dirs.append(client_dir_name)

    if args.max_rows is not None:
        download_and_distribute(KAGGLE_EN_FILE_BASE_PATH, client_dirs, "en", args.max_rows)
        download_and_distribute(KAGGLE_FR_FILE_BASE_PATH, client_dirs, "fr", args.max_rows)
    else:
        download_and_distribute(KAGGLE_EN_FILE_BASE_PATH, client_dirs, "en", MAX_ROWS)
        download_and_distribute(KAGGLE_FR_FILE_BASE_PATH, client_dirs, "fr", MAX_ROWS)


if __name__ == "__main__":
    main()