"""
run_all.py  -  start the whole pipeline with one command, stop it with Ctrl+C.

    python run_all.py              start everything (containers must already be up)
    python run_all.py --clean      FIRST wipe everything (docker down -v, rm -rf data), start the
                                   containers, then start everything.  Use it for a fresh run.
    python run_all.py --down       when you stop, also run `docker compose down` (keeps the volumes)
    python run_all.py --keep-flink when you stop, leave the Flink jobs running

Order (the same as the README):
    1. DataGenerator.py                 (the world)
    2. KafkaProducer.py                 (the bridge; we wait for "[FOLLOWING]" = topics exist)
    3. the 4 Flink SQL jobs             (submitted one after the other)
    4. Dispatcher.py

Every script's output is shown with a prefix [gen] [bridge] [disp] and is also saved in logs/.
Ctrl+C stops the three Python scripts (in reverse order) and cancels the Flink jobs.
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from dotenv import dotenv_values

HERE = Path(__file__).resolve().parent
COMPOSE_FILE = "Flink-Kafka-docker-compose.yml"
JOBMANAGER = "flink-jobmanager"
SQL_FILES = ["01_bronze.sql", "02_silver.sql", "03_detection.sql", "04_replenishment.sql"]
LOG_DIR = HERE / "logs"

procs = []   # (name, Popen), in start order

def load_project_env():
    values = dotenv_values(HERE / ".env")

    # Apply project settings, including FLEET_SIZE=30.
    for key, value in values.items():
        if value is not None:
            os.environ[key] = value

    # Use the project's data folder unless .env explicitly specifies DATA_DIR.
    if not values.get("DATA_DIR"):
        os.environ["DATA_DIR"] = str(HERE / "data")

def run(cmd, check=True, quiet=False):
    """Run a command to the end and return its output."""
    result = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)
    if not quiet and result.stdout.strip():
        print(result.stdout.rstrip())
    if check and result.returncode != 0:
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(cmd)}\n{result.stderr.strip()}")
    return result


def start_script(name, script, ready_text=None, ready_timeout=60):
    """Start a python script, show its output with a prefix, optionally wait until it prints ready_text."""
    LOG_DIR.mkdir(exist_ok=True)
    log = (LOG_DIR / f"{name}.log").open("w", encoding="utf-8")
    p = subprocess.Popen(
        [sys.executable, "-u", script],
        cwd=HERE,
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        creationflags=(
            subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        ),
    )
    procs.append((name, p))
    ready = threading.Event()

    def pump():
        for line in p.stdout:
            line = line.rstrip("\n")
            print(f"[{name}] {line}", flush=True)
            log.write(line + "\n")
            log.flush()
            if ready_text and ready_text in line:
                ready.set()
        log.close()

    threading.Thread(target=pump, daemon=True).start()
    if ready_text:
        deadline = time.monotonic() + ready_timeout
        while not ready.wait(0.5):
            if p.poll() is not None:
                raise RuntimeError(f"{script} stopped before it was ready (see logs/{name}.log)")
            if time.monotonic() > deadline:
                raise RuntimeError(f"{script} was not ready after {ready_timeout} s (see logs/{name}.log)")
    return p


def submit_flink_jobs():
    for sql in SQL_FILES:
        print(f"[flink] submitting {sql} ...", flush=True)
        r = run(["docker", "exec", JOBMANAGER, "./bin/sql-client.sh", "-f", f"/opt/flink/sql/{sql}"],
                check=False, quiet=True)
        tail = (r.stdout + r.stderr).strip().splitlines()[-3:]
        if r.returncode != 0:
            raise RuntimeError(f"Flink job {sql} failed:\n" + "\n".join(tail))
        print(f"[flink] {sql} submitted", flush=True)


def cancel_flink_jobs():
    r = run(["docker", "exec", JOBMANAGER, "./bin/flink", "list", "-r"], check=False, quiet=True)
    for line in r.stdout.splitlines():
        # "25.10.2026 23:00:00 : <job id> : <name> (RUNNING)"
        parts = [x.strip() for x in line.split(" : ")]
        if len(parts) >= 3 and "(RUNNING)" in parts[2]:
            run(["docker", "exec", JOBMANAGER, "./bin/flink", "cancel", parts[1]], check=False, quiet=True)
            print(f"[flink] cancelled {parts[2]}", flush=True)


def stop_scripts():
    for name, p in reversed(procs):
        if p.poll() is None:
            print(f"[stop] {name}", flush=True)
            try:
                if os.name == "nt":
                    p.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    p.send_signal(signal.SIGINT)
            except (OSError, ValueError):
                p.terminate()

    for name, p in reversed(procs):
        try:
            p.wait(timeout=15)
        except subprocess.TimeoutExpired:
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

def clean():
    print("[clean] docker compose down -v", flush=True)
    run(["docker", "compose", "-f", COMPOSE_FILE, "down", "-v"])
    shutil.rmtree(HERE / "data", ignore_errors=True)
    print("[clean] data/ removed. Starting containers...", flush=True)
    run(["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "--build"])
    print("[clean] waiting 20 s for Kafka / Flink / HDFS to be ready...", flush=True)
    time.sleep(20)


def main():
    load_project_env()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--clean", action="store_true", help="wipe docker volumes and data/, then start everything")
    ap.add_argument("--down", action="store_true", help="run `docker compose down` when stopping")
    ap.add_argument("--keep-flink", action="store_true", help="do not cancel the Flink jobs when stopping")
    args = ap.parse_args()

    try:
        if args.clean:
            clean()
        start_script("gen", "DataGenerator.py", ready_text="Waiting for shipments")
        start_script("bridge", "KafkaProducer.py", ready_text="[FOLLOWING]")
        submit_flink_jobs()
        start_script("disp", "Dispatcher.py", ready_text="State ready")
        print("\n=== everything is running. Press Ctrl+C to stop. ===\n", flush=True)
        while True:
            for name, p in procs:
                if p.poll() is not None:
                    raise RuntimeError(f"{name} stopped by itself (exit code {p.returncode}), see logs/{name}.log")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...", flush=True)
    except RuntimeError as exc:
        print(f"\n[ERROR] {exc}", flush=True)
    finally:
        signal.signal(signal.SIGINT, signal.SIG_IGN)     # a second Ctrl+C must not break the clean-up
        stop_scripts()
        if not args.keep_flink:
            cancel_flink_jobs()
        if args.down:
            run(["docker", "compose", "-f", COMPOSE_FILE, "down"], check=False)
        print("All stopped.", flush=True)


if __name__ == "__main__":
    os.chdir(HERE)
    main()
