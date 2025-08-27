"""
JAQL core pipeline operations: select, project, derive (updated)
Enhanced with better expression evaluation for JC compatibility
"""

import json
from typing import Any, Dict, List, Union
from .utils import safe_eval, deep_get

def normalize_to_records(data: Any) -> List[Dict[str, Any]]:
    """Normalize input to list of records for processing"""
    if isinstance(data, dict):
        return [data]
    elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
        return data
    else:
        # Wrap non-dict data as a record
        return [{"value": data}]

def apply_pipeline(data: Any, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply a sequence of pipe operations to data
    
    Args:
        data: Input data (dict, list of dicts, or other)
        pipeline: List of pipe operations from YAML config
        
    Returns:
        List of transformed records
    """
    # Normalize input to list of records
    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        records = data  # Already normalized
    else:
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
    Enhanced with better expression evaluation for JC data
    
    Args:
        records: List of record dictionaries
        expression: Python expression to evaluate (e.g., "age >= 18", "rec['load-state'] == 'loaded'")
        
    Returns:
        Filtered list of records
    """
    result = []
    for record in records:
        try:
            # Use enhanced safe_eval with 'rec' binding and 'get' helper
            if safe_eval(expression, record):
                result.append(record)
        except Exception:
            # Skip records that cause evaluation errors
            continue
    return result

def pipe_project(records: List[Dict[str, Any]], fields: List[str]) -> List[Dict[str, Any]]:
    """
    Select specific fields from records (π operation)
    Enhanced with nested field support
    
    Args:
        records: List of record dictionaries
        fields: List of field names to keep (supports dot notation like 'user.name')
        
    Returns:
        Records with only the specified fields
    """
    result = []
    for record in records:
        projected = {}
        for field in fields:
            if '.' in field:
                # Handle nested field access
                value = deep_get(record, field)
                if value is not None:
                    # Preserve the full path in the output
                    projected[field] = value
            else:
                # Simple field access
                if field in record:
                    projected[field] = record[field]
        result.append(projected)
    return result

def pipe_derive(records: List[Dict[str, Any]], derivations: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Add new fields computed from expressions
    Enhanced with better expression evaluation
    
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
            try:
                # Use enhanced safe_eval with 'rec' binding and 'get' helper
                new_record[field_name] = safe_eval(expression, record)
            except Exception:
                # Set to None if derivation fails
                new_record[field_name] = None
        result.append(new_record)
    return result
