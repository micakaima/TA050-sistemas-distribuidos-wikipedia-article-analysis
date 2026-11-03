import difflib, argparse, pathlib

# Permite comparar 2 archivos individuales.
#   - Se debe pasar como argumento '-f1' el path del archivo 1.
#   - Se debe pasar como argumento '-f2' el path del archivo 2.
def parse_arguments():
    parser = argparse.ArgumentParser(description="Compares two files")
    parser.add_argument("-f1", "--file1", type=str, required=True, help="File 1 path")
    parser.add_argument("-f2", "--file2", type=str, required=True, help="File 2 path")
    return parser.parse_args()


def main():
    args = parse_arguments()

    path1 = pathlib.Path(args.file1)
    path2 = pathlib.Path(args.file2)
    if not path1.exists() or not path2.exists():
        print(f"[ERROR] Paths are incorrect")
        return

    with open(path1) as file_1:
        file_1_text = file_1.readlines()

    with open(path2) as file_2:
        file_2_text = file_2.readlines()

    total_different_lines = 0
    for line in difflib.unified_diff(file_1_text, file_2_text, fromfile=str(path1), tofile=str(path2), lineterm=''):
        total_different_lines += 1
        print(line)

    if total_different_lines == 0:
        print("Files are identical!")

if __name__ == "__main__":
    main()