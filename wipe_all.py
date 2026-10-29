"""
wipe_all.py  -  delete EVERYTHING so the next run starts from zero.

    python wipe_all.py              full wipe, then stop (start again with: docker compose up -d --build)
    python wipe_all.py --up         full wipe, then start the containers again (empty)
    python wipe_all.py --hdfs-only  keep Kafka and the containers, only empty HDFS (Bronze + Silver)
    python wipe_all.py --yes        do not ask "are you sure?"

What a full wipe deletes
    * the Flink jobs (cancelled)
    * all containers of this project and ALL their volumes:
          Kafka (every topic and message, consumer offsets)
          HDFS  (hdfs-name + hdfs-data  = Bronze and Silver Iceberg tables)
          Flink (checkpoints + savepoints)
          Prometheus + Grafana (metrics history)
          Airflow (Postgres metadata DB, logs) and the Gold tables (they live in HDFS)
    * the app-data volume (csv + jsonl from the generator), plus data/ and logs/ if they exist
It does NOT delete your code, .env, or the Docker images.

The generator, bridge and dispatcher are containers now: the full wipe stops them with everything else.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent

ENV = os.environ.copy()          # Linux: docker socket + group id for the Airflow containers
_sock = Path("/var/run/docker.sock")
if sys.platform.startswith("linux") and _sock.exists():
    ENV.setdefault("DOCKER_SOCK", str(_sock))
    ENV.setdefault("DOCKER_GID", str(_sock.stat().st_gid))
COMPOSE_FILE = "docker-compose.yml"
VOLUMES = ["hdfs-name", "hdfs-data", "flink-checkpoints", "flink-savepoints", "prometheus-data", "grafana-data",
           "app-data", "airflow-db", "airflow-logs"]


def run(cmd, check=False):
    r = subprocess.run(cmd, cwd=HERE, env=ENV, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}\n{r.stderr.strip() or r.stdout.strip()}")
    return r


def say(msg):
    print(msg, flush=True)


def container_running(name):
    r = run(["docker", "ps", "--format", "{{.Names}}"])
    return name in r.stdout.split()


def cancel_flink_jobs():
    if not container_running("flink-jobmanager"):
        return
    r = run(["docker", "exec", "flink-jobmanager", "./bin/flink", "list", "-r"])
    n = 0
    for line in r.stdout.splitlines():
        parts = [x.strip() for x in line.split(" : ")]
        if len(parts) >= 3 and "(RUNNING)" in parts[2]:
            run(["docker", "exec", "flink-jobmanager", "./bin/flink", "cancel", parts[1]])
            n += 1
    say(f"[flink] cancelled {n} running job(s)")


def wipe_hdfs_only():
    if not container_running("namenode"):
        raise RuntimeError("namenode is not running. Use the full wipe instead (no --hdfs-only).")
    # platform_guard would start the Flink jobs again within 5 minutes: pause both DAGs first
    if container_running("airflow-scheduler"):
        for dag in ("platform_guard", "lakehouse_batch"):
            run(["docker", "exec", "airflow-scheduler", "airflow", "dags", "pause", dag])
        say("[airflow] paused platform_guard and lakehouse_batch")
    run(["docker", "compose", "-f", COMPOSE_FILE, "stop", "dispatcher", "bridge", "generator"])
    say("[apps] stopped dispatcher, bridge, generator")
    cancel_flink_jobs()
    time.sleep(3)
    say("[hdfs] deleting /warehouse/bronze, silver and gold ...")
    run(["docker", "exec", "namenode", "hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash",
         "/warehouse/bronze", "/warehouse/silver", "/warehouse/gold"])
    run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p",
         "/warehouse/bronze", "/warehouse/silver", "/warehouse/gold"], check=True)
    run(["docker", "exec", "namenode", "hdfs", "dfs", "-chmod", "-R", "777", "/warehouse"])
    r = run(["docker", "exec", "namenode", "hdfs", "dfs", "-du", "-h", "/warehouse"])
    say("[hdfs] now:\n" + (r.stdout.strip() or "(empty)"))
    say("Kafka was NOT touched, so the old topics will replay into the new Bronze.")
    say("Start again:  docker compose start generator bridge dispatcher")
    say("then:         docker exec airflow-scheduler airflow dags unpause platform_guard   (and lakehouse_batch)")


def wipe_everything(start_again):
    cancel_flink_jobs()
    say("[docker] docker compose down -v --remove-orphans")
    run(["docker", "compose", "-f", COMPOSE_FILE, "down", "-v", "--remove-orphans"], check=True)

    # leftovers: volumes whose name ends with one of ours (any compose project name)
    r = run(["docker", "volume", "ls", "-q"])
    left = [v for v in r.stdout.split() if any(v == x or v.endswith("_" + x) for x in VOLUMES)]
    for v in left:
        say(f"[docker] removing leftover volume {v}")
        run(["docker", "volume", "rm", "-f", v])

    for folder in ("data", "logs"):
        shutil.rmtree(HERE / folder, ignore_errors=True)
        say(f"[files] removed {folder}/")

    r = run(["docker", "volume", "ls", "-q"])
    still = [v for v in r.stdout.split() if any(v == x or v.endswith("_" + x) for x in VOLUMES)]
    if still:
        raise RuntimeError("these volumes could not be removed: " + ", ".join(still))
    say("[check] no project volumes left: Kafka, HDFS and Flink state are gone.")

    if start_again:
        say("[docker] starting fresh containers ...")
        run(["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "--build"], check=True)
        say("Containers started (empty). Airflow platform_guard submits the 4 Flink jobs within about 5 minutes.")
    else:
        say("Done. Start again with: docker compose -f docker-compose.yml up -d --build")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--up", action="store_true", help="start the containers again after the wipe")
    ap.add_argument("--hdfs-only", action="store_true", help="only empty HDFS, keep Kafka and the containers")
    ap.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    args = ap.parse_args()

    what = "ALL data in HDFS (Bronze, Silver, Gold)" if args.hdfs_only else \
           "ALL data: Kafka topics, HDFS (Bronze, Silver, Gold), Flink state, Airflow history, data/ and logs/"
    if not args.yes and input(f"This deletes {what}. Type YES to continue: ").strip() != "YES":
        say("Cancelled.")
        return
    try:
        wipe_hdfs_only() if args.hdfs_only else wipe_everything(args.up)
    except RuntimeError as exc:
        say(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
