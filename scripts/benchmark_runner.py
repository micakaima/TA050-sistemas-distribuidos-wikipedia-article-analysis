#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime

# Script automatizador de benchmarks para el sistema distribuido de análisis de artículos de Wikipedia.
# Automatiza:
# 1. Generación de entradas particionadas usando volumen (MB/GB) o filas, con caché local / Kaggle fallback.
# 2. Ejecución del oráculo secuencial para obtener el patrón de verdad y medir T_secuencial.
# 3. Despliegue de Docker Compose y seguimiento de los clientes distribuidos para medir T_distribuido.
# 4. Verificación de integridad comparando salidas con el oráculo.
# 5. Generación de reporte completo con métricas de volumen (MB), Throughput (MB/s y arts/s) y Speedup.

DEFAULT_ROWS = 50
DEFAULT_CLIENTS = 3
DEFAULT_CACHE_DIR = "./data/cache"
DEFAULT_OUTPUT_REPORT = "./data/benchmark_report.md"
DEFAULT_TIMEOUT = 600

def parse_args():
    parser = argparse.ArgumentParser(
        description="Wikipedia Article Analysis - Automated Benchmark & Test Suite Runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--volume", type=str, default=None, help="Target total dataset volume (e.g. 50MB, 100MB, 1GB)")
    parser.add_argument("--volume-per-client", type=str, default=None, help="Target volume per client (e.g. 20MB, 50MB)")
    parser.add_argument("-r", "--rows", type=int, default=DEFAULT_ROWS, help="Complete rows per file/language (used if volume is not set)")
    parser.add_argument("-c", "--clients", type=int, default=DEFAULT_CLIENTS, help="Number of concurrent clients")
    parser.add_argument("--cache-dir", type=str, default=DEFAULT_CACHE_DIR, help="Directory for cached parquet datasets")
    parser.add_argument("--no-cache", action="store_true", help="Disable cache and force download from Kaggle")
    parser.add_argument("--skip-oracle", action="store_true", help="Skip running the sequential Go oracle")
    parser.add_argument("--skip-docker", action="store_true", help="Skip running the distributed Docker Compose stack")
    parser.add_argument("--skip-teardown", action="store_true", help="Do not stop Docker Compose containers at completion")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Max timeout in seconds waiting for clients")
    parser.add_argument("-o", "--output-report", type=str, default=DEFAULT_OUTPUT_REPORT, help="Path for markdown benchmark report")
    return parser.parse_args()

def log_section(title):
    print("\n" + "=" * 80)
    print(f"  {title.upper()}")
    print("=" * 80)

def run_command(cmd, desc="", capture_output=False, check=True):
    if desc:
        print(f"[*] {desc}")
    print(f"    > {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, text=True, capture_output=capture_output)

def get_input_stats(clients):
    total_bytes = 0
    rows_per_file = 0
    total_files = 0
    for client_id in range(clients):
        input_dir = f"data/input/input_{client_id}"
        if not os.path.exists(input_dir):
            continue
        for fname in sorted(os.listdir(input_dir)):
            fpath = os.path.join(input_dir, fname)
            if os.path.isfile(fpath) and fname.endswith(".parquet"):
                total_bytes += os.path.getsize(fpath)
                total_files += 1
                if rows_per_file == 0:
                    try:
                        import pyarrow.parquet as pq
                        meta = pq.read_metadata(fpath)
                        rows_per_file = meta.num_rows
                    except Exception:
                        pass
    return total_bytes, rows_per_file, total_files

def stage_generate_inputs(args):
    log_section("Paso 1: Generación de Datasets de Entrada")
    gen_cmd = [
        sys.executable,
        "scripts/generate_short_input.py",
        "--clients", str(args.clients),
        "--cache-dir", args.cache_dir,
    ]

    if args.volume:
        gen_cmd.extend(["--volume", str(args.volume)])
        desc_vol = f"volumen total de {args.volume}"
    elif args.volume_per_client:
        gen_cmd.extend(["--volume-per-client", str(args.volume_per_client)])
        desc_vol = f"volumen de {args.volume_per_client}/cliente"
    else:
        gen_cmd.extend(["-max", str(args.rows)])
        desc_vol = f"{args.rows} filas completas/idioma"

    if args.no_cache:
        gen_cmd.append("--no-cache")

    start = time.time()
    run_command(gen_cmd, desc=f"Generando entradas con {desc_vol} x {args.clients} clientes")
    elapsed = time.time() - start

    total_bytes, rows_per_file, total_files = get_input_stats(args.clients)
    total_mb = total_bytes / (1024 * 1024)
    total_articles = rows_per_file * total_files if rows_per_file > 0 else (args.rows * 2 * args.clients)

    print(f"[+] Datasets listos en {elapsed:.2f} s: {total_mb:.2f} MB ({total_articles:,} artículos en {total_files} archivos)")
    return elapsed, total_bytes, rows_per_file, total_articles

def stage_run_oracle(args, rows_per_file):
    log_section("Paso 2: Ejecución de Oráculo Secuencial (Baseline)")
    if args.skip_oracle:
        print("[!] Oráculo omitido (--skip-oracle activado).")
        return None

    rows_arg = str(rows_per_file if rows_per_file > 0 else args.rows)
    total_oracle_time = 0.0
    for client_id in range(args.clients):
        input_dir = f"data/input/input_{client_id}"
        expected_dir = f"data/expected_results/results_{client_id}"
        os.makedirs(expected_dir, exist_ok=True)

        oracle_cmd = [
            "go", "run", "scripts/solve_sequencially.go",
            input_dir,
            expected_dir,
            rows_arg,
        ]
        start = time.time()
        run_command(oracle_cmd, desc=f"Calculando Ground Truth secuencial para cliente {client_id} ({rows_arg} filas)")
        elapsed = time.time() - start
        total_oracle_time += elapsed
        print(f"[+] Oráculo para cliente {client_id} finalizado en {elapsed:.2f} s")

    print(f"[+] Tiempo total de Oráculo Secuencial: {total_oracle_time:.2f} s")
    return total_oracle_time

def stage_run_distributed(args):
    log_section("Paso 3: Despliegue y Ejecución Distribuida (Docker Compose)")
    if args.skip_docker:
        print("[!] Ejecución distribuida omitida (--skip-docker activado).")
        return None, {}

    # Limpiar directorio de salidas previas para no leer resultados viejos
    for client_id in range(args.clients):
        out_dir = f"data/output/output_{client_id}"
        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.makedirs(out_dir, exist_ok=True)

    # Levantar workers y middleware primero
    up_cmd = ["docker", "compose", "up", "-d", "--remove-orphans"]
    run_command(up_cmd, desc="Iniciando clúster distribuido en background")

    # Detener clientes temporalmente y limpiar almacenamiento
    client_names = [f"client_{i}" for i in range(args.clients)]
    subprocess.run(["docker", "compose", "stop"] + client_names, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "run", "--rm", "-v", f"{os.getcwd()}/data:/data", "alpine", "sh", "-c", "rm -rf /data/storage/*"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)

    # Iniciar clientes y comenzar cronómetro
    run_command(["docker", "compose", "up", "-d"] + client_names, desc=f"Iniciando {len(client_names)} clientes concurrentes")
    print(f"[*] Monitoreando finalización de clientes: {', '.join(client_names)}")

    start_time = time.time()
    deadline = start_time + args.timeout
    finished_clients = set()
    exit_codes = {}

    while time.time() < deadline:
        for client_name in client_names:
            if client_name in finished_clients:
                continue

            inspect_cmd = [
                "docker", "inspect",
                "--format", "{{.State.Status}}|{{.State.ExitCode}}",
                client_name,
            ]
            res = subprocess.run(inspect_cmd, capture_output=True, text=True)
            if res.returncode == 0:
                parts = res.stdout.strip().split("|")
                status = parts[0]
                exit_code = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else -1

                if status == "exited":
                    finished_clients.add(client_name)
                    exit_codes[client_name] = exit_code
                    t_client = time.time() - start_time
                    print(f"[*] [{t_client:.1f}s] {client_name} finalizó con exit code: {exit_code}")

        if len(finished_clients) == len(client_names):
            break

        time.sleep(1.0)

    total_dist_time = time.time() - start_time

    if len(finished_clients) < len(client_names):
        pending = set(client_names) - finished_clients
        print(f"[!] ALERTA: Timeout de {args.timeout}s alcanzado. Clientes pendientes: {pending}")

    print(f"[+] Tiempo total de ejecución distribuida: {total_dist_time:.2f} s")
    return total_dist_time, exit_codes

def stage_verify_results(args):
    log_section("Paso 4: Validación de Resultados vs Oráculo")
    verification_results = {}
    all_matched = True

    for client_id in range(args.clients):
        client_results = {}
        for q in range(1, 6):
            file_out = f"data/output/output_{client_id}/query_{q}_result.txt"
            file_exp = f"data/expected_results/results_{client_id}/query_{q}_result.txt"

            if not os.path.exists(file_out):
                client_results[f"Q{q}"] = "OUTPUT_MISSING"
                all_matched = False
                continue
            if not os.path.exists(file_exp):
                client_results[f"Q{q}"] = "EXPECTED_MISSING"
                all_matched = False
                continue

            cmp_cmd = [
                sys.executable,
                "scripts/compare_specific_outputs.py",
                "-f1", file_out,
                "-f2", file_exp,
            ]
            res = subprocess.run(cmp_cmd, capture_output=True, text=True)
            if res.returncode == 0:
                client_results[f"Q{q}"] = "OK"
            else:
                client_results[f"Q{q}"] = "DIFF"
                all_matched = False

        verification_results[client_id] = client_results

    return verification_results, all_matched

def stage_teardown(args):
    if args.skip_docker or args.skip_teardown:
        return
    log_section("Paso 5: Apagado y Limpieza de Contenedores")
    down_cmd = ["docker", "compose", "down"]
    run_command(down_cmd, desc="Deteniendo clúster distribuido")

def generate_report(args, gen_time, total_bytes, rows_per_file, total_articles, oracle_time, dist_time, exit_codes, verification_results, all_matched):
    log_section("Resumen del Benchmark")
    total_mb = total_bytes / (1024 * 1024) if total_bytes > 0 else 0.0
    mb_per_client = (total_mb / args.clients) if args.clients > 0 else total_mb

    speedup = (oracle_time / dist_time) if (oracle_time and dist_time and dist_time > 0) else None
    throughput_arts = (total_articles / dist_time) if (dist_time and dist_time > 0) else None
    throughput_mb = (total_mb / dist_time) if (dist_time and dist_time > 0) else None

    # Resumen en terminal
    print("Métricas de Datos:")
    print(f"  - Volumen Total de Entrada: {total_mb:.2f} MB ({total_bytes:,} bytes)")
    print(f"  - Volumen por Cliente:      {mb_per_client:.2f} MB (~{rows_per_file:,} filas completas/idioma)")
    print(f"  - Clientes Concurrentes:    {args.clients}")
    print(f"  - Total Artículos Proc.:    {total_articles:,}")

    print("\nTiempos y Rendimiento:")
    if oracle_time is not None:
        print(f"  - T. Secuencial (Oráculo):  {oracle_time:.2f} s")
    if dist_time is not None:
        print(f"  - T. Distribuido:           {dist_time:.2f} s")
    if speedup is not None:
        print(f"  - Speedup Factor:           {speedup:.2f}x")
    if throughput_mb is not None:
        print(f"  - Throughput de Datos:      {throughput_mb:.2f} MB/s")
    if throughput_arts is not None:
        print(f"  - Throughput de Artículos:  {throughput_arts:.1f} arts/s")

    print("\nResultados de Validación:")
    header = f"{'Cliente':<10} | {'Q1':<6} | {'Q2':<6} | {'Q3':<6} | {'Q4':<6} | {'Q5':<6} | {'Estado':<10}"
    print("-" * len(header))
    print(header)
    print("-" * len(header))

    for client_id, q_res in verification_results.items():
        q1 = q_res.get("Q1", "-")
        q2 = q_res.get("Q2", "-")
        q3 = q_res.get("Q3", "-")
        q4 = q_res.get("Q4", "-")
        q5 = q_res.get("Q5", "-")
        client_ok = all(v == "OK" for v in q_res.values()) and len(q_res) == 5
        status_str = "SUCCESS" if client_ok else "FAILED"
        print(f"{f'client_{client_id}':<10} | {q1:<6} | {q2:<6} | {q3:<6} | {q4:<6} | {q5:<6} | {status_str:<10}")
    print("-" * len(header))

    # Guardar reporte Markdown
    os.makedirs(os.path.dirname(os.path.abspath(args.output_report)), exist_ok=True)
    with open(args.output_report, "w") as f:
        f.write("# Reporte de Benchmark - Wikipedia Article Analysis\n\n")
        f.write(f"**Fecha y Hora:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("## 1. Configuración de la Prueba\n\n")
        f.write(f"- **Volumen Total de Entrada:** `{total_mb:.2f} MB` ({total_bytes:,} bytes)\n")
        f.write(f"- **Volumen por Cliente:** `{mb_per_client:.2f} MB`\n")
        f.write(f"- **Filas completas por archivo:** `{rows_per_file:,}` (artículos íntegros sin fragmentar)\n")
        f.write(f"- **Total artículos en sistema:** `{total_articles:,}` artículos (idiomas `en` y `fr`)\n")
        f.write(f"- **Clientes concurrentes:** {args.clients}\n")
        f.write(f"- **Modo de caché:** {'Habilitado' if not args.no_cache else 'Deshabilitado'}\n\n")

        f.write("## 2. Métricas de Rendimiento\n\n")
        f.write("| Métrica | Valor |\n")
        f.write("| :--- | :--- |\n")
        if oracle_time is not None:
            f.write(f"| **Tiempo Secuencial (Oráculo Go)** | `{oracle_time:.2f} s` |\n")
        if dist_time is not None:
            f.write(f"| **Tiempo Distribuido (Docker)** | `{dist_time:.2f} s` |\n")
        if speedup is not None:
            f.write(f"| **Speedup Factor** | `{speedup:.2f}x` |\n")
        if throughput_mb is not None:
            f.write(f"| **Throughput de Datos** | `{throughput_mb:.2f} MB/s` |\n")
        if throughput_arts is not None:
            f.write(f"| **Throughput de Artículos** | `{throughput_arts:.1f} artículos/s` |\n")
        f.write(f"| **Tiempo Generación de Inputs** | `{gen_time:.2f} s` |\n\n")

        f.write("## 3. Validación de Resultados vs Oráculo\n\n")
        f.write("| Cliente | Q1 | Q2 | Q3 | Q4 | Q5 | Estado Global |\n")
        f.write("| :--- | :---: | :---: | :---: | :---: | :---: | :---: |\n")
        for client_id, q_res in verification_results.items():
            client_ok = all(v == "OK" for v in q_res.values()) and len(q_res) == 5
            icon = "✅ PASS" if client_ok else "❌ FAIL"
            f.write(f"| `client_{client_id}` | `{q_res.get('Q1', '-')}` | `{q_res.get('Q2', '-')}` | `{q_res.get('Q3', '-')}` | `{q_res.get('Q4', '-')}` | `{q_res.get('Q5', '-')}` | **{icon}** |\n")

        f.write(f"\n**Resultado General:** {'✅ Todos los resultados coinciden con el oráculo.' if all_matched else '❌ Se detectaron discrepancias en las salidas.'}\n")

    print(f"\n[+] Reporte completo guardado en: {args.output_report}")

def main():
    args = parse_args()

    gen_time, total_bytes, rows_per_file, total_articles = stage_generate_inputs(args)
    oracle_time = stage_run_oracle(args, rows_per_file)
    dist_time, exit_codes = stage_run_distributed(args)
    verification_results, all_matched = stage_verify_results(args)
    stage_teardown(args)

    generate_report(args, gen_time, total_bytes, rows_per_file, total_articles, oracle_time, dist_time, exit_codes, verification_results, all_matched)

    if not all_matched:
        sys.exit(1)

if __name__ == "__main__":
    main()
