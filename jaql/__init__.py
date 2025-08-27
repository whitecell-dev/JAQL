"""
JAQL: JSON + YAML Query Language
A jq-like tool using YAML pipelines for JSON transformation

Core operations: select, project, derive
Pure functional transformations with no side effects
"""

from .pipes import apply_pipeline
from .runner import run_pipeline
from .parser import load_pipeline

__all__ = ["apply_pipeline", "run_pipeline", "load_pipeline"]
