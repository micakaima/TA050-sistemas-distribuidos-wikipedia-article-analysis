import subprocess, sys, argparse, os, json
from update_docker_compose import flatten_dict

CONFIG_FILE_PATH = "./scripts/configs/compare_all_outputs_config.json"

def load_config(config_path=CONFIG_FILE_PATH):
    if not os.path.exists(config_path):
        raise FileNotFoundError("Config file not found")
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return flatten_dict(data)

# Permite comparar todas las querys results para 1 cliente.
#   - Los resultados generados deben estar en data/output/output_{ID}
#   - Los resultados esperados deben estar en data/expected_results/results_{ID}
#   - Se debe pasar como argumento el ID.
def parse_arguments():
    parser = argparse.ArgumentParser(description="Compares all querys results for 1 client")
    parser.add_argument("-c", "--client", type=str, required=True, help="Client ID")
    return parser.parse_args()

def main():
    config = load_config()
    args = parse_arguments()

    all_matched = True

    for i in range(1, 6):
        file1 = f"{config["base_dir1"]}/output_{args.client}/query_{i}_result.txt"
        file2 = f"{config["base_dir2"]}/results_{args.client}/query_{i}_result.txt"
        
        print(f" Analizando Query {i}: ")
        print(f" - File 1: {file1}")
        print(f" - File 2: {file2}")
        
        if i == 2:
            cmp_script = "scripts/compare_q2_deterministic.py"
        else:
            cmp_script = "scripts/compare_specific_outputs.py"

        result = subprocess.run([
            sys.executable, cmp_script, 
            "-f1", file1, 
            "-f2", file2
        ])
        if result.returncode != 0:
            all_matched = False
        print()

    sys.exit(0 if all_matched else 1)

if __name__ == "__main__":
    main()