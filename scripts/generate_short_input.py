import argparse
import os
import shutil
import kagglehub
import pyarrow.parquet as pq

KAGGLE_DATASET_BASE_PATH = "wikimedia-foundation/wikipedia-structured-contents"
KAGGLE_EN_FILE_BASE_PATH = "enwiki/data/enwiki_namespace_0_"
KAGGLE_FR_FILE_BASE_PATH = "frwiki/data/frwiki_namespace_0_"
INPUT_BASE_PATH = "./data/input"
CACHE_BASE_PATH = "./data/cache"

NUM_CLIENTS = 3
MAX_ROWS = 30
# Factores de densidad promedio de Wikipedia (bytes comprimidos por fila completa)
BYTES_PER_ROW_EN = 2391.2
BYTES_PER_ROW_FR = 793.5
BYTES_PER_COMBINED_ROW = BYTES_PER_ROW_EN + BYTES_PER_ROW_FR  # ~3184.7 bytes

def parse_volume_to_bytes(vol_str):
    if not vol_str:
        return None
    vol_str = vol_str.strip().upper()
    units = {
        "KB": 1024,
        "K": 1024,
        "MB": 1024 * 1024,
        "M": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "G": 1024 * 1024 * 1024,
        "B": 1,
    }
    for unit, multiplier in sorted(units.items(), key=lambda x: -len(x[0])):
        if vol_str.endswith(unit):
            num_part = vol_str[:-len(unit)].strip()
            try:
                return int(float(num_part) * multiplier)
            except ValueError:
                break
    try:
        return int(float(vol_str))
    except ValueError:
        raise ValueError(f"Formato de volumen inválido: '{vol_str}'. Ejemplos válidos: 50MB, 100M, 1.5GB, 500KB")

# Permite generar archivos parquet de input truncados para un cliente/s con soporte de caché offline y definición por volumen.
#   - Permite especificar el volumen directamente con '-v' / '--volume' (ej. 50MB) o '--volume-per-client' (ej. 20MB).
#   - Calcula la cantidad de filas COMPLETAS requeridas garantizando que ningún registro quede truncado al medio.
#   - Prioriza archivos pre-descargados en data/cache con fallback a kagglehub.
def parse_arguments():
    parser = argparse.ArgumentParser(description="Generator of short datasets with cache and volume support")
    parser.add_argument("-c", "--client", type=int, default=None, help="Client ID (default: all clients)")
    parser.add_argument("-max", "--max_rows", type=int, default=None, help="Max complete rows per file")
    parser.add_argument("-v", "--volume", type=str, default=None, help="Total target dataset volume (e.g. 50MB, 100MB, 1GB)")
    parser.add_argument("--volume-per-client", type=str, default=None, help="Target volume per client (e.g. 20MB, 50MB)")
    parser.add_argument("--clients", type=int, default=NUM_CLIENTS, help=f"Number of clients (default: {NUM_CLIENTS})")
    parser.add_argument("--cache-dir", type=str, default=CACHE_BASE_PATH, help=f"Cache directory (default: {CACHE_BASE_PATH})")
    parser.add_argument("--no-cache", action="store_true", help="Disable cache and force download via kagglehub")
    return parser.parse_args()


def backup_existing_inputs_to_cache(cache_dir):
    if not os.path.exists(INPUT_BASE_PATH):
        return
    os.makedirs(cache_dir, exist_ok=True)
    for root, _, files in os.walk(INPUT_BASE_PATH):
        for file in files:
            if file.endswith(".parquet") and not file.endswith("-truncated.parquet"):
                src_path = os.path.join(root, file)
                dest_path = os.path.join(cache_dir, file)
                if not os.path.exists(dest_path) and os.path.getsize(src_path) > 1024 * 1024:
                    print(f"[CACHE] Preserving existing dataset into cache: {file} ({os.path.getsize(src_path)} bytes)")
                    shutil.copy2(src_path, dest_path)

def clean_old_inputs():
    if os.path.exists(INPUT_BASE_PATH):
        shutil.rmtree(INPUT_BASE_PATH)
    os.makedirs(INPUT_BASE_PATH)

def create_client_dir(client_id):
    client_dir_name = os.path.join(INPUT_BASE_PATH, f"input_{client_id}")
    os.makedirs(client_dir_name, exist_ok=True)
    return client_dir_name

def get_dataset_file(file_base_path, idx, cache_dir, use_cache=True):
    file_name = f"{file_base_path}{idx:05}.parquet"
    base_name = os.path.basename(file_name)
    cached_file = os.path.join(cache_dir, base_name)

    # 1. Preferencia: Archivo exacto en cache
    if use_cache and os.path.exists(cached_file) and os.path.getsize(cached_file) > 0:
        print(f"[CACHE] Found exact cached file: {cached_file}")
        return cached_file

    # 1.b. Fallback dentro de cache: Si no existe el idx específico pero sí el índice 00000
    base_prefix = os.path.basename(file_base_path)
    fallback_cached = os.path.join(cache_dir, f"{base_prefix}00000.parquet")
    if use_cache and os.path.exists(fallback_cached) and os.path.getsize(fallback_cached) > 0:
        print(f"[CACHE] Using index 0 cached file as fallback for index {idx}: {fallback_cached}")
        return fallback_cached

    # 2. Fallback externo: Descargar con kagglehub
    print(f"[KAGGLE] Not found in cache. Downloading {file_name} from Kaggle...")
    try:
        dataset_path = kagglehub.dataset_download(KAGGLE_DATASET_BASE_PATH, path=file_name)
        if use_cache:
            os.makedirs(cache_dir, exist_ok=True)
            shutil.copy2(dataset_path, cached_file)
            print(f"[CACHE] Saved downloaded file to cache: {cached_file}")
        return dataset_path
    except Exception as e:
        print(f"[ERROR] While downloading {file_name}: {e}")
        # Si falló la descarga pero tenemos algún archivo de caché disponible para ese prefijo
        if use_cache and os.path.exists(fallback_cached):
            print(f"[CACHE] Network failed, falling back to cached file: {fallback_cached}")
            return fallback_cached
        raise

def download_and_distribute(file_base_path, client_dirs, lang, max_rows, cache_dir, use_cache):
    idx = 0
    for _, target_dir in enumerate(client_dirs):
        try:
            source_file = get_dataset_file(file_base_path, idx, cache_dir, use_cache)
            output_filename = os.path.join(target_dir, f"{lang}-truncated.parquet")
            written = truncate_parquet_file(source_file, output_filename, max_rows)
            file_size_mb = os.path.getsize(output_filename) / (1024 * 1024)
            print(f"[INPUT] Created {output_filename}: {written:,} complete rows ({file_size_mb:.2f} MB)")
        except Exception as e:
            print(f"[ERROR] While processing {file_base_path} for {target_dir}: {e}")
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
    return rows_written

def main():
    args = parse_arguments()
    use_cache = not args.no_cache

    num_clients = 1 if args.client is not None else args.clients

    # Determinar cantidad de filas completas a generar
    target_bytes_per_client = None
    if args.volume_per_client:
        target_bytes_per_client = parse_volume_to_bytes(args.volume_per_client)
    elif args.volume:
        total_bytes = parse_volume_to_bytes(args.volume)
        target_bytes_per_client = int(total_bytes / num_clients)

    if target_bytes_per_client is not None:
        # Cada cliente recibe 1 archivo EN y 1 archivo FR con filas completas
        # Tamaño total esperado = filas * (BYTES_PER_ROW_EN + BYTES_PER_ROW_FR)
        computed_rows = max(1, round(target_bytes_per_client / BYTES_PER_COMBINED_ROW))
        rows_to_generate = computed_rows
        target_mb = (target_bytes_per_client * num_clients) / (1024 * 1024)
        print(f"[VOLUMEN] Objetivo: {target_mb:.2f} MB total (~{target_bytes_per_client / (1024*1024):.2f} MB/cliente) -> {rows_to_generate:,} filas completas por archivo")
    elif args.max_rows is not None:
        rows_to_generate = args.max_rows
    else:
        rows_to_generate = MAX_ROWS

    if use_cache:
        backup_existing_inputs_to_cache(args.cache_dir)

    clean_old_inputs()

    client_dirs = []
    if args.client is not None:
        client_dirs.append(create_client_dir(args.client))
    else:
        for c in range(args.clients):
            client_dirs.append(create_client_dir(c))

    download_and_distribute(KAGGLE_EN_FILE_BASE_PATH, client_dirs, "en", rows_to_generate, args.cache_dir, use_cache)
    download_and_distribute(KAGGLE_FR_FILE_BASE_PATH, client_dirs, "fr", rows_to_generate, args.cache_dir, use_cache)

    total_bytes = 0
    total_files = 0
    for c_dir in client_dirs:
        for fname in os.listdir(c_dir):
            fpath = os.path.join(c_dir, fname)
            if os.path.isfile(fpath):
                total_bytes += os.path.getsize(fpath)
                total_files += 1

    total_mb = total_bytes / (1024 * 1024)
    print(f"[INPUT RESUMEN] Total generado: {total_mb:.2f} MB en {total_files} archivos ({total_bytes:,} bytes)")

if __name__ == "__main__":
    main()