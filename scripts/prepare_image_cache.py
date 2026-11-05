#!/usr/bin/env python3
"""
Precalentador de caché de imágenes para Wikipedia Article Analysis.
Descarga y almacena en data/cache/images/ las imágenes (.img) o confirmaciones
de no-imagen (.noimage) para todos los artículos biográficos (<= 2020) del dataset de entrada.
"""

import argparse
import datetime
import glob
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
import pyarrow.parquet as pq

USER_AGENT = "Wikipedia-Article-Analysis/1.0 (academic-testing; fiuba-distribuidos)"
EXTERNAL_API_ERROR = "External API error: could not retrieve image"

def parse_args():
    parser = argparse.ArgumentParser(description="Precalentar caché de imágenes de Wikipedia")
    parser.add_argument("--input-dir", type=str, default="data/input", help="Directorio con subcarpetas input_*")
    parser.add_argument("--cache-dir", type=str, default="data/cache/images", help="Directorio de destino para la caché")
    parser.add_argument("--delay", type=float, default=0.25, help="Pausa entre peticiones en segundos")
    parser.add_argument("--max-retries", type=int, default=3, help="Máximo de reintentos por petición")
    return parser.parse_args()

def save_cache_file(cache_dir, filename, content):
    os.makedirs(cache_dir, exist_ok=True)
    target_path = os.path.join(cache_dir, filename)
    tmp_path = f"{target_path}.tmp.{time.time_ns()}"
    with open(tmp_path, "wb") as f:
        f.write(content if isinstance(content, bytes) else content.encode("utf-8"))
    os.replace(tmp_path, target_path)

def request_bytes(url, max_retries=3, delay=0.25):
    for attempt in range(max_retries):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
                body = resp.read()
                return body, status, None
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None, 404, None
            if e.code == 429:
                retry_after = e.headers.get("Retry-After", "10")
                try:
                    sec = int(retry_after)
                except ValueError:
                    sec = 10
                sec = min(sec, 30)  # Limitar espera razonable
                print(f"      [429 Too Many Requests] Esperando {sec}s...")
                time.sleep(sec)
                continue
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) * 0.5)
                continue
            return None, e.code, e
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) * 0.5)
                continue
            return None, 0, e
    return None, 0, Exception("Max retries exceeded")

def fetch_and_cache(cand, cache_dir, delay):
    aid = cand["id"]
    img_name = f"{aid}.img"
    noimg_name = f"{aid}.noimage"

    # 1. Si viene con direct image_url
    if cand["image_url"]:
        body, status, err = request_bytes(cand["image_url"])
        time.sleep(delay)
        if status == 200 and body and len(body) > 0:
            save_cache_file(cache_dir, img_name, body)
            return "IMG (direct)"

    # 2. Consultar API REST de Wikipedia
    parts = cand["url"].split("/wiki/")
    if len(parts) != 2:
        save_cache_file(cache_dir, noimg_name, EXTERNAL_API_ERROR)
        return "NOIMAGE (invalid url)"

    title = parts[1]
    language = "en"
    if ".fr.wikipedia.org" in cand["url"]:
        language = "fr"
    # Mantener PathEscape idéntico a Go url.PathEscape
    escaped_title = urllib.parse.quote(title, safe="")
    api_url = f"https://{language}.wikipedia.org/api/rest_v1/page/summary/{escaped_title}"

    body, status, err = request_bytes(api_url)
    time.sleep(delay)

    if status == 404:
        save_cache_file(cache_dir, noimg_name, EXTERNAL_API_ERROR)
        return "NOIMAGE (404 page)"

    if status != 200 or not body:
        # Fallo transitorio de red -> no cachear para que se pueda reintentar
        return f"SKIP (transient error status={status})"

    try:
        summary = json.loads(body.decode("utf-8"))
    except Exception:
        save_cache_file(cache_dir, noimg_name, EXTERNAL_API_ERROR)
        return "NOIMAGE (json parse error)"

    thumb = summary.get("thumbnail")
    thumb_src = thumb.get("source") if isinstance(thumb, dict) else None
    if not thumb_src:
        save_cache_file(cache_dir, noimg_name, EXTERNAL_API_ERROR)
        return "NOIMAGE (no thumbnail)"

    # 3. Descargar el thumbnail
    img_body, img_status, img_err = request_bytes(thumb_src)
    time.sleep(delay)

    if img_status == 404:
        save_cache_file(cache_dir, noimg_name, EXTERNAL_API_ERROR)
        return "NOIMAGE (404 thumb)"

    if img_status == 200 and img_body and len(img_body) > 0:
        save_cache_file(cache_dir, img_name, img_body)
        return "IMG (api thumbnail)"

    return f"SKIP (transient thumb error status={img_status})"

def main():
    args = parse_args()
    os.makedirs(args.cache_dir, exist_ok=True)

    print("=" * 80)
    print("  PREPARADOR DE CACHÉ DE IMÁGENES")
    print("=" * 80)
    print(f"Buscando archivos parquet en: {args.input_dir}...")

    # Extraer candidatos biográficos
    cached_files = set(os.listdir(args.cache_dir)) if os.path.exists(args.cache_dir) else set()
    print(f"Archivos existentes en caché: {len(cached_files)}")

    candidates = {}
    parquet_files = sorted(glob.glob(f"{args.input_dir}/*/*.parquet"))
    if not parquet_files:
        parquet_files = sorted(glob.glob(f"{args.input_dir}/*.parquet"))

    print(f"Encontrados {len(parquet_files)} archivos parquet.")

    for pfile in parquet_files:
        try:
            table = pq.read_table(pfile, columns=["identifier", "name", "url", "image", "sections", "date_modified"])
        except Exception as e:
            print(f"[!] Error leyendo {pfile}: {e}")
            continue

        for row in table.to_pylist():
            dt = row["date_modified"]
            if isinstance(dt, (int, float)):
                year = datetime.datetime.fromtimestamp(dt / 1000.0, tz=datetime.timezone.utc).year
            elif hasattr(dt, "year"):
                year = dt.year
            else:
                year = 0

            sec = (row["sections"] or "").lower()
            if year <= 2020 and ("biography" in sec or "biographie" in sec):
                aid = str(row["identifier"])
                if aid not in candidates:
                    img_dict = row["image"] or {}
                    candidates[aid] = {
                        "id": aid,
                        "name": row["name"],
                        "url": row["url"],
                        "image_url": img_dict.get("content_url") if isinstance(img_dict, dict) else None,
                    }

    total_candidates = len(candidates)
    missing = [c for aid, c in candidates.items() if f"{aid}.img" not in cached_files and f"{aid}.noimage" not in cached_files]

    print(f"Total biografías encontradas: {total_candidates}")
    print(f"En caché actualmente:         {total_candidates - len(missing)}")
    print(f"Faltantes a procesar:         {len(missing)}")

    if not missing:
        print("\n[+] ¡La caché ya está 100% completa! No hay nada que descargar.")
        return

    print(f"\n[*] Iniciando descarga y resolución de {len(missing)} artículos con delay={args.delay}s...\n")
    start_time = time.time()
    success_count = 0

    for idx, cand in enumerate(missing, 1):
        res = fetch_and_cache(cand, args.cache_dir, args.delay)
        if "IMG" in res or "NOIMAGE" in res:
            success_count += 1
        print(f"[{idx:3d}/{len(missing):3d}] Art: {cand['id']:<10} | {cand['name'][:30]:<30} -> {res}")

    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"[+] Finalizado en {elapsed:.1f} s. {success_count}/{len(missing)} artículos guardados en caché.")
    total_final = len(os.listdir(args.cache_dir))
    print(f"Total elementos en {args.cache_dir}: {total_final}")
    print("=" * 80)

if __name__ == "__main__":
    main()
