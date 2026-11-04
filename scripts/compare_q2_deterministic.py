import argparse
import re
import sys
from pathlib import Path 

RECORD_START = re.compile(rb"^\d+,[^,\r\n]*,\s*https?://")


def split_records(data):
    records, current = [], None
    for line in data.split(b"\n"):
        if RECORD_START.match(line):
            if current is not None:
                records.append(b"\n".join(current))
            current = [line]
        elif current is not None:
            current.append(line)
    if current is not None:
        records.append(b"\n".join(current))
    return records


def parse(rec):
    parts = rec.split(b",", 3)
    if len(parts) < 4:
        return None
    id, name, url, rest = parts
    image, _, error = rest.rpartition(b",")
    return (
        id.strip(),
        name.strip(),
        url.strip(),
        len(image) > 0,
        len(error.strip()) > 0,
    )


def load(path):
    parsed = [parse(r) for r in split_records(path.read_bytes())]
    parsed = [p for p in parsed if p is not None] 
    parsed.sort(key=lambda t: t[0])
    return parsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-f1", "--file1", required=True)
    ap.add_argument("-f2", "--file2", required=True)
    args = ap.parse_args()

    p1, p2 = Path(args.file1), Path(args.file2)
    if not p1.exists() or not p2.exists():
        print("Archivo no encontrado")
        return 1

    r1, r2 = load(p1), load(p2)
    print(f"file1: {len(r1)} registros | file2: {len(r2)} registros")
    print(f"con imagen: file1={sum(t[3] for t in r1)} | file2={sum(t[3] for t in r2)}")
    print(f"con error:  file1={sum(t[4] for t in r1)} | file2={sum(t[4] for t in r2)}")

    if len(r1) != len(r2):
        print("Cantidad de registros distinta")
        return 1

    diff = 0
    for a, b in zip(r1, r2):
        if a != b:
            print(f"distinto: {a} vs {b}")
            diff += 1

    print("OK" if diff == 0 else f"{diff} diferencias")
    return 0 if diff == 0 else 1


if __name__ == "__main__":
    sys.exit(main())