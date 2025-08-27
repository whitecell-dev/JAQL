"""
JAQL pipeline parser and validator
"""

import yaml
from typing import Dict, List, Any

def load_pipeline(path: str) -> List[Dict[str, Any]]:
    """
    Load and validate YAML pipeline configuration
    """
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        raise ValueError(f"Pipeline file not found: {path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {path}: {e}")
    
    if not isinstance(config, dict):
        raise ValueError("Pipeline config must be a YAML object")
    
    if 'pipes' not in config:
        raise ValueError("Pipeline config must contain 'pipes' key")
    
    pipes = config['pipes']
    if not isinstance(pipes, list):
        raise ValueError("'pipes' must be a list of operations")
    
    # Basic validation of pipe operations
    valid_operations = {'select', 'project', 'derive'}
    
    for i, pipe in enumerate(pipes):
        if not isinstance(pipe, dict):
            raise ValueError(f"Pipe {i}: must be an object")
        
        pipe_ops = set(pipe.keys())
        if not pipe_ops.issubset(valid_operations):
            invalid_ops = pipe_ops - valid_operations
            raise ValueError(f"Pipe {i}: unknown operations {invalid_ops}")
        
        if len(pipe_ops) != 1:
            raise ValueError(f"Pipe {i}: each pipe must contain exactly one operation")
    
    return pipes

def validate_pipeline(pipeline: List[Dict[str, Any]]) -> List[str]:
    """
    Validate pipeline operations and return list of error messages
    """
    errors = []
    
    for i, pipe in enumerate(pipeline):
        if 'select' in pipe:
            if not isinstance(pipe['select'], str):
                errors.append(f"Pipe {i}: 'select' must be a string expression")
        
        elif 'project' in pipe:
            if not isinstance(pipe['project'], list):
                errors.append(f"Pipe {i}: 'project' must be a list of field names")
            elif not all(isinstance(field, str) for field in pipe['project']):
                errors.append(f"Pipe {i}: 'project' fields must be strings")
        
        elif 'derive' in pipe:
            if not isinstance(pipe['derive'], dict):
                errors.append(f"Pipe {i}: 'derive' must be an object mapping field names to expressions")
            elif not all(isinstance(k, str) and isinstance(v, str) for k, v in pipe['derive'].items()):
                errors.append(f"Pipe {i}: 'derive' keys and values must be strings")
    
    return errors
