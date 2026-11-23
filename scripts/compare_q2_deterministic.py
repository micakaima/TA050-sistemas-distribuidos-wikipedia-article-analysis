import argparse
import re
import sys
from pathlib import Path

RECORD_START = re.compile(rb"^\d+,[^,\r\n]*,\s*https?://")

ERROR_RULES = [
    (b"Internal error", "permanent"),
    (b"External API error: Status Not Found (Error 404)", "error_404"),
    (b"External API error: Failed all attempts for download", "failed_all_attempts"),
    (b"External API error: Missing Image URL Error", "missing_url"),
]

def is_error(payload: bytes):
    stripped = payload.lstrip()
    for prefix, _ in ERROR_RULES:
        if stripped.startswith(prefix):
            return True
    return False

def classify_error(payload: bytes):
    stripped = payload.lstrip()
    for prefix, category in ERROR_RULES:
        if stripped.startswith(prefix):
            return category
    return "unknown_error"

# Devuelve si el error es tolerable. Tiene en cuenta:
#   - Si el flag allow_external_errors esta activado.
#   - Solo acepta errores de la API externa.
def is_tolerable_error(err_kind, allow_external_errors):
    if not allow_external_errors or err_kind is None:
        return False
    return (
        err_kind == "error_404" or
        err_kind == "failed_all_attempts" or
        err_kind == "missing_url"
    )


# Detecta y devuelve el formato de la imagen (bytes).
def detect_image_format(data: bytes):
    if not data:
        return None
    if data.startswith(b"\xff\xd8\xff"):
        return "JPEG"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "GIF"
    if len(data) >= 12 and data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "WebP"
    if data.startswith(b"BM"):
        return "BMP"
    if data.startswith((b"II*\x00", b"MM\x00*")):
        return "TIFF"
    return None


# Calcula la proporcion de bytes distintos entre la imagen A y B (en bytes).
# Recorre la cantidad de bytes minima entre ambas imagenes, compara byte a byte si son iguales,
# y devuelve la cantidad de bytes distintos por sobre la totalidad de bytes maxima.
def byte_diff_ratio(a: bytes, b: bytes):
    max_len = max(len(a), len(b))
    if max_len == 0:
        return 0.0
    min_len = min(len(a), len(b))
    diff_bytes = sum(1 for i in range(min_len) if a[i] != b[i])
    diff_bytes += max_len - min_len
    return diff_bytes / max_len


# Si dos imagenes A y B (bytes) no pasan la validacion, genera un resumen con ciertos datos:
#   - Muestra la comparacion entre cantidad de bytes de A y B.
#   - Muestra la comparacion entre el formato de A y B.
def visual_diagnostics(a_bytes: bytes, b_bytes: bytes):
    fmt_a = detect_image_format(a_bytes) or "unknown"
    fmt_b = detect_image_format(b_bytes) or "unknown"
    len_a, len_b = len(a_bytes), len(b_bytes)
    parts = [
        f"bytes={len_a} vs {len_b}",
        f"fmt={fmt_a} vs {fmt_b}",
    ]
    return ", ".join(parts)


# Recorre el archivo linea a linea en bytes, y devuelve la lista de cada registro/fila
# resultado en bytes.
def split_records(data: bytes):
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


# Parsea un registro a un diccionario.
# Incluye si la query devolvio error (y que tipo) o si devolvio una imagen valida.
def parse(rec: bytes):
    parts = rec.split(b",", 3)
    if len(parts) < 4:
        return None
    id, name, url, payload = parts
    has_err = is_error(payload)

    if has_err:
        err_kind = classify_error(payload)
        return {
            "id": id.strip(),
            "name": name.strip(),
            "url": url.strip(),
            "has_valid_image": False,
            "has_error": True,
            "error_kind": err_kind,
            "payload": payload,
        }

    fmt = detect_image_format(payload)
    valid_img = len(payload) > 0 and fmt is not None
    return {
        "id": id.strip(),
        "name": name.strip(),
        "url": url.strip(),
        "has_valid_image": valid_img,
        "has_error": False,
        "error_kind": None,
        "payload": payload,
    }


# Lee los bytes del archivo, parsea todos los registros a diccionarios, y los guarda en
# un diccionario indexado por id.
def load(path):
    records = {}
    for r in split_records(path.read_bytes()):
        p = parse(r)
        if p is not None:
            records[p["id"]] = p
    return records


# Evalua un par de registros (en formato diccionario tras parsearlos) con el mismo ArticleID
# para tres casos:
#   - Caso 1: Ambos devolvieron sin error.
#       * Chequea que ambas imagenes sean validas (longitud>0, formato reconocible).
#       * Chequea que la proporcion de bytes desiguales entre ellos no supera el umbral de tolerancia.
#       * Si no son imagenes validas o no cumplen el umbral, muestra un resumen de ciertos datos (long, formato).
#   - Caso 2: Ambos devolvieron con error.
#       * Chequea si ambos errores sean tolerables (flag activado, sea un error de API externo)
#       * Chequea si ambos son el mismo tipo de error.
#   - Caso 3: Uno devuelve error y el otro no.
#       * Chequea que el de sin error tenga una imagen valida.
#       * Chequea que el que tiene error sea un error tolerable.
def compare_pair(
    a: dict,
    b: dict,
    tolerance: float,
    allow_external_errors: bool,
):
    # Caso 1
    if not a["has_error"] and not b["has_error"]:
        return handle_no_errors(a, b, tolerance)
    # Caso 2
    if a["has_error"] and b["has_error"]:
        return handle_both_errors(a, b, allow_external_errors)
    # Caso 3
    return handle_only_one_error(a, b, allow_external_errors)


def handle_no_errors(a: dict, b: dict, tolerance: float):
    # Chequea si ambas imagenes son validas
    if not a["has_valid_image"] or not b["has_valid_image"]:
        diag = visual_diagnostics(a["payload"], b["payload"])
        return False, f"Invalid or Empty Image (f1_valid={a['has_valid_image']}, f2_valid={b['has_valid_image']}) | {diag}"
    
    # Chequea si la proporcion de bytes diferentes supera el umbral de tolerancia
    ratio = byte_diff_ratio(a["payload"], b["payload"])
    if ratio > tolerance:
        diag = visual_diagnostics(a["payload"], b["payload"])
        return False, f"Different Bytes Rate {ratio:.2%} > Tolerance {tolerance:.2%} | {diag}"
    return True, ""


def handle_both_errors(a: dict, b: dict, allow_external_errors: bool):
    # Chequea si ambos errores son tolerables
    if is_tolerable_error(a["error_kind"], allow_external_errors) and is_tolerable_error(b["error_kind"], allow_external_errors):
        return True, ""
    
    # Chequea si ambos son el mismo tipo de error
    if a["error_kind"] == b["error_kind"]:
        return True, ""
    return False, f"Error types are different: ({a['error_kind']} vs {b['error_kind']})"


def handle_only_one_error(a: dict, b: dict, allow_external_errors: bool):
    other = b if a["has_error"] else a
    err = a if a["has_error"] else b
    # Chequea que el de sin error tenga una imagen valida + Chequea que el que tiene error sea un error tolerable.
    if is_tolerable_error(err["error_kind"], allow_external_errors) and other["has_valid_image"]:
        return True, ""
    return False, f"Inconsistency: Error vs Image (err_kind={err['error_kind']}, valid_img={other['has_valid_image']})"


# Da detalles de que tipo de errores surgieron durante la query.
def error_breakdown(records: dict[bytes, dict]) -> str:
    kinds = ["permanent", "error_404", "failed_all_attempts", "missing_url", "unknown_error"]
    counts = {k: sum(1 for x in records.values() if x["error_kind"] == k) for k in kinds}
    return (
        f"perm={counts['permanent']}, "
        f"error_404={counts['error_404']}, "
        f"failed_all_attemps={counts['failed_all_attempts']}, "
        f"missing_url={counts['missing_url']}, "
        f"unk={counts['unknown_error']}"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Compares results of query 2 with byte-level tolerance.")
    ap.add_argument("-f1", "--file1", required=True)
    ap.add_argument("-f2", "--file2", required=True)
    ap.add_argument("--tolerance", type=float, default=0.05, help="Maximum tolerance threshold for distinct bytes (ej. 0.05 = 5%%)")
    ap.add_argument("--allow-external-errors", type=bool, default=True, help="Allow External API errors ('External API error: ...')")
    ap.add_argument("--verbose", type=bool, default=True, help="Verbose")
    args = ap.parse_args()

    p1, p2 = Path(args.file1), Path(args.file2)
    if not p1.exists() or not p2.exists():
        print("File not found")
        return 1

    d1, d2 = load(p1), load(p2)
    all_ids = sorted(set(d1) | set(d2))

    if args.verbose:
        print(f"file1: {len(d1)} registros | file2: {len(d2)} registros")
        print(f"file1 valid images: {sum(1 for x in d1.values() if x['has_valid_image'])} | file2 valid images: {sum(1 for x in d2.values() if x['has_valid_image'])}")
        print(f"file1 errors: [{error_breakdown(d1)}] | file2 errors: [{error_breakdown(d2)}]")

    if not all_ids:
        print("Results are correct!")
        return 0

    ok_count = 0
    for art_id in all_ids:
        a, b = d1.get(art_id), d2.get(art_id)
        if a is None or b is None:
            print(f"[FAIL] id={art_id.decode(errors='replace')}: missing in {'file2' if b is None else 'file1'}")
            continue

        passed, reason = compare_pair(a, b, args.tolerance, args.allow_external_errors)
        if passed:
            ok_count += 1
        else:
            print(f"[FAIL] id={art_id.decode(errors='replace')} ({a['name'].decode(errors='replace')}): {reason}")

    pass_rate = ok_count / len(all_ids)
    query_ok = pass_rate >= 1.0
    print(f"Result: {ok_count}/{len(all_ids)} OK -> {"Results are correct!" if query_ok else "Results are NOT correct!"}")
    return 0 if query_ok else 1


if __name__ == "__main__":
    sys.exit(main())