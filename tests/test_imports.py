"""Verify all Python files are syntactically valid."""
import importlib
import os
import ast
import sys

SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts")
CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config")

def test_scripts_parse():
    """All scripts in scripts/ should parse as valid Python."""
    for fname in sorted(os.listdir(SCRIPTS_DIR)):
        if fname.endswith(".py"):
            path = os.path.join(SCRIPTS_DIR, fname)
            with open(path) as f:
                ast.parse(f.read())
            # If we get here, the file is syntactically valid

def test_config_parse():
    """All modules in config/ should parse as valid Python."""
    for fname in sorted(os.listdir(CONFIG_DIR)):
        if fname.endswith(".py"):
            path = os.path.join(CONFIG_DIR, fname)
            with open(path) as f:
                ast.parse(f.read())

def test_airflow_dag_parse():
    """The DAG file should parse as valid Python."""
    dag_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "airflow", "dags", "insurance_pipeline_dag.py"
    )
    with open(dag_path, encoding="utf-8") as f:
        ast.parse(f.read())

def test_etl_pipeline_pattern():
    """Each ETL script should have the standard 6-step pattern."""
    etl_scripts = ["08_load_fact_sales.py", "09_load_fact_claims.py", "etl_fact_payments.py"]
    for fname in etl_scripts:
        path = os.path.join(SCRIPTS_DIR, fname)
        with open(path) as f:
            content = f.read()
        assert "extract()" in content or "def extract" in content
        assert "validate" in content
        assert "transform" in content
        assert "deduplicate" in content or "dedupe" in content
        assert "load" in content
        assert "verify" in content
