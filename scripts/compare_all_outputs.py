import subprocess, sys, argparse

# Permite comparar todas las querys results para 1 cliente.
#   - Los resultados generados deben estar en data/output/output_{ID}
#   - Los resultados esperados deben estar en data/expected_results/results_{ID}
#   - Se debe pasar como argumento el ID.
def parse_arguments():
    parser = argparse.ArgumentParser(description="Compares all querys results for 1 client")
    parser.add_argument("-c", "--client", type=str, required=True, help="Client ID")
    return parser.parse_args()

def main():
    args = parse_arguments()

    all_matched = True

    for i in range(1, 6):
        file1 = f"data/output/output_{args.client}/query_{i}_result.txt"
        file2 = f"data/expected_results/results_{args.client}/query_{i}_result.txt"
        
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