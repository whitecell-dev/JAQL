"""
JAQL core pipeline operations: select, project, derive
Pure functional transformations on JSON data
"""

import json
from typing import Any, Dict, List, Union
from .utils import safe_eval, normalize_to_records

def apply_pipeline(data: Any, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply a sequence of pipe operations to JSON data
    
    Args:
        data: Input JSON data (dict, list of dicts, or other)
        pipeline: List of pipe operations from YAML config
        
    Returns:
        List of transformed records
    """
    # Normalize input to list of records
    records = normalize_to_records(data)
    
    # Apply each pipe operation in sequence
    for pipe in pipeline:
        if 'select' in pipe:
            records = pipe_select(records, pipe['select'])
        elif 'project' in pipe:
            records = pipe_project(records, pipe['project'])
        elif 'derive' in pipe:
            records = pipe_derive(records, pipe['derive'])
    
    return records

def pipe_select(records: List[Dict[str, Any]], expression: str) -> List[Dict[str, Any]]:
    """
    Filter records based on expression (σ operation)
    
    Args:
        records: List of record dictionaries
        expression: Python expression to evaluate (e.g., "age >= 18")
        
    Returns:
        Filtered list of records
    """
    result = []
    for record in records:
        if safe_eval(expression, record):
            result.append(record)
    return result

def pipe_project(records: List[Dict[str, Any]], fields: List[str]) -> List[Dict[str, Any]]:
    """
    Select specific fields from records (π operation)
    
    Args:
        records: List of record dictionaries
        fields: List of field names to keep
        
    Returns:
        Records with only the specified fields
    """
    result = []
    for record in records:
        projected = {}
        for field in fields:
            if field in record:
                projected[field] = record[field]
        result.append(projected)
    return result

def pipe_derive(records: List[Dict[str, Any]], derivations: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Add new fields computed from expressions
    
    Args:
        records: List of record dictionaries
        derivations: Dict mapping new field names to expressions
        
    Returns:
        Records with additional derived fields
    """
    result = []
    for record in records:
        # Create new record with derived fields
        new_record = dict(record)
        for field_name, expression in derivations.items():
            new_record[field_name] = safe_eval(expression, record)
        result.append(new_record)
    return result
