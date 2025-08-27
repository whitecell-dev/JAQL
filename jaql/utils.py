"""
JAQL utilities: enhanced safe evaluation with better key access
Supports hyphenated keys from jc output via 'rec' binding and 'get()' helper
"""

import ast
from typing import Any, Dict, List, Union

def safe_eval(expr: str, context: Dict[str, Any]) -> Any:
    """
    Safely evaluate expressions against context with limited built-ins
    Enhanced with 'rec' binding and 'get' helper for hyphenated keys
    """
    try:
        parsed = ast.parse(expr, mode='eval')

        # Allow only safe built-ins
        safe_builtins = {
            "len": len,
            "min": min,
            "max": max,
            "sum": sum,
            "abs": abs,
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
        }

        # Create safe environment with context variables
        safe_globals = {"__builtins__": safe_builtins}
        
        # Add all context variables directly (for bare variable access)
        safe_globals.update(context)
        
        # Add 'rec' binding for explicit record access
        safe_globals["rec"] = context
        
        # Add 'get' helper function for safe key access
        safe_globals["get"] = lambda key, default=None: context.get(key, default)

        return eval(compile(parsed, '<string>', 'eval'), safe_globals)
    except Exception:
        # Return False for any evaluation errors (used in select operations)
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
    """
    Normalize input to list of records for processing
    NOTE: This duplicates functionality from io.py but is kept for backward compatibility
    """
    if isinstance(data, dict):
        return [data]
    elif is_list_of_dicts(data):
        return data
    else:
        # Wrap non-dict data as a record
        return [{"value": data}]

# Test function for validation
def test_safe_eval_context_resolution():
    """Test enhanced expression evaluation"""
    # Test bare variable access
    assert safe_eval("age >= 18", {"age": 20}) is True
    
    # Test built-in functions
    assert safe_eval("len(name)", {"name": "Alice"}) == 5
    
    # Test 'rec' binding for hyphenated keys
    assert safe_eval("rec['load-state'] == 'loaded'", {"load-state": "loaded"}) is True
    
    # Test 'get' helper
    assert safe_eval("get('load-state') == 'loaded'", {"load-state": "loaded"}) is True
    assert safe_eval("get('nonexistent', 'default')", {}) == 'default'
    
    # Test error handling
    assert safe_eval("nonexistent", {}) is False

if __name__ == "__main__":
    test_safe_eval_context_resolution()
    print("All tests passed!")

