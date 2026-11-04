# projects/project1/dag_def/project1_parser.py
from pathlib import Path
import logging

import yaml
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from pendulum import datetime

from pipeline_orchestrator.paths import PROJECTS_DIR

log = logging.getLogger(__name__)

CONFIG_DIR = PROJECTS_DIR / "project_sync_test" / "dags" / "dag_configs"

IMAGE_REPO = "your-registry.example.com/project1"
NAMESPACE = "airflow"
PROJECT_NAME = "project1"


def _load_yaml_configs() -> list[dict]:
    configs = []

    if not CONFIG_DIR.exists():
        log.warning("Config dir not found: %s", CONFIG_DIR)
        return configs

    for yaml_file in sorted(CONFIG_DIR.glob("*.yaml")):
        try:
            with open(yaml_file) as f:
                cfg = yaml.safe_load(f)
        except Exception:
            log.exception("Failed to parse %s", yaml_file)
            continue

        configs.append(cfg)

    return configs


def _build_dag(cfg: dict) -> DAG:
    dag_id = f"{PROJECT_NAME}_{cfg['dag_name']}"
    tasks_cfg = cfg.get("tasks", [])

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["project1"],
    ) as dag:
        task_ops = {}
        for task_entry in tasks_cfg:
            for task_name, task_params in task_entry.items():
                image_tag = task_params.get("image_tag")
                if not image_tag:
                    log.warning("Task %s in %s missing image_tag, skipping", task_name, dag_id)
                    continue

                task_ops[task_name] = KubernetesPodOperator(
                    task_id=task_name,
                    name=f"{dag_id}-{task_name}".replace("_", "-"),
                    namespace=NAMESPACE,
                    image=f"{IMAGE_REPO}/{task_name}:{image_tag}",
                    is_delete_operator_pod=True,
                    get_logs=True,
                )

        ordered = list(task_ops.values())
        for upstream, downstream in zip(ordered, ordered[1:]):
            upstream >> downstream

    return dag


def get_dags() -> dict:
    dags = {}
    logging.info('Parsing project 1')
    yaml_dags = _load_yaml_configs()
    if len(yaml_dags) == 0:
        logging.info('No Dags Found')
        return dags

    for cfg in yaml_dags: 
        try:
            dag = _build_dag(cfg)
            dags[dag.dag_id] = dag
        except Exception:
            log.exception("Failed to build DAG from config: %s", cfg.get("dag_id", "unknown"))
    return dags

globals().update(get_dags())