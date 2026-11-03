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

    for i in range(1, 6):
        file1 = f"data/output/output_{args.client}/query_{i}_result.txt"
        file2 = f"data/expected_results/results_{args.client}/query_{i}_result.txt"
        
        print(f" Analizando Query {i}: ")
        print(f" - File 1: {file1}")
        print(f" - File 2: {file2}")
        
        result = subprocess.run([
            sys.executable, "scripts/compare_specific_outputs.py", 
            "-f1", file1, 
            "-f2", file2
        ])
        print()

if __name__ == "__main__":
    main()