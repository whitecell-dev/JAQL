"""
JAQL utilities: safe evaluation, deep get operations, etc.
"""

import ast
from typing import Any, Dict, List, Union

def safe_eval(expr: str, context: Dict[str, Any]) -> Any:
    """
    Safely evaluate expressions against context
    Returns False for any evaluation errors to prevent crashes
    """
    try:
        # Parse expression to AST for basic safety
        parsed = ast.parse(expr, mode='eval')
        
        # Use eval with restricted context (no builtins)
        return eval(compile(parsed, '<string>', 'eval'), {"__builtins__": {}}, context)
    except Exception:
        # Return False for any errors - prevents crashes on bad data/expressions
        return False

def deep_get(obj: Dict[str, Any], path: str, default=None) -> Any:
    """
    Get nested values using dot notation like 'user.profile.name'
    """
    try:
        current = obj
        for part in path.split('.'):
            if isinstance(current, dict):
                current = current[part]
            else:
                return default
        return current
    except (KeyError, TypeError):
        return default

def deep_set(obj: Dict[str, Any], path: str, value: Any) -> None:
    """
    Set nested values using dot notation
    """
    parts = path.split('.')
    current = obj
    
    for part in parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]
    
    current[parts[-1]] = value

def is_list_of_dicts(data: Any) -> bool:
    """Check if data is a list containing only dictionaries"""
    return isinstance(data, list) and all(isinstance(item, dict) for item in data)

def normalize_to_records(data: Any) -> List[Dict[str, Any]]:
    """Normalize input to list of records for processing"""
    if isinstance(data, dict):
        return [data]
    elif is_list_of_dicts(data):
        return data
    else:
        # Wrap non-dict data as a record
        return [{"value": data}]

