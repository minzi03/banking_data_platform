"""
YAML configuration loader utilities.
Hỗ trợ Jinja template rendering cho parameterized configs.
"""

import os
import yaml
from pathlib import Path
from jinja2 import Template


def load_config(config_path: str) -> dict:
    """Load YAML configuration file (no rendering)."""
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if config is None:
        raise ValueError(f"Empty or invalid YAML file: {config_path}")

    return config


def load_config_pipeline(config_path: str, spark=None, context_vars: dict = None) -> dict:
    """
    Load YAML config with optional Jinja rendering.

    Deploy mode is resolved from SparkConf when spark is provided:
      - client : reads file directly from the given path
      - cluster: resolves relative paths from os.getcwd() (driver CWD on cluster)

    context_vars: dict of Jinja variables rendered into the raw YAML before
    parsing (e.g. {"cob_dt": "2024-01-01"}).
    """
    deploy_mode = "client"
    if spark:
        deploy_mode = spark.sparkContext.getConf().get("spark.submit.deployMode", "client")

    if deploy_mode == "cluster" and not os.path.isabs(config_path):
        full_path = os.path.join(os.getcwd(), config_path)
    else:
        full_path = config_path

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Config file not found: {full_path}")

    with open(full_path, "r", encoding="utf-8") as f:
        raw = f.read()

    if context_vars:
        raw = Template(raw).render(**context_vars)

    config = yaml.safe_load(raw)
    if config is None:
        raise ValueError(f"Empty or invalid YAML: {full_path}")

    return config
