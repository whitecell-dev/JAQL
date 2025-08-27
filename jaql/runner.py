"""
JAQL pipeline runner: loads pipeline and executes on JSON input
"""

import sys
import json
from typing import Any

from .parser import load_pipeline, validate_pipeline
from .pipes import apply_pipeline

def run_pipeline(pipeline_path: str, input_file: str = None, output_file: str = None) -> None:
    """
    Run a JAQL pipeline on JSON input
    
    Args:
        pipeline_path: Path to YAML pipeline file
        input_file: Path to input JSON file (None for stdin)
        output_file: Path to output file (None for stdout)
    """
    try:
        # Load and validate pipeline
        pipeline = load_pipeline(pipeline_path)
        validation_errors = validate_pipeline(pipeline)
        
        if validation_errors:
            print("Pipeline validation errors:", file=sys.stderr)
            for error in validation_errors:
                print(f"  - {error}", file=sys.stderr)
            sys.exit(1)
        
        # Load input data
        if input_file:
            with open(input_file, 'r') as f:
                data = json.load(f)
        else:
            try:
                data = json.load(sys.stdin)
            except json.JSONDecodeError as e:
                print(f"Invalid JSON input: {e}", file=sys.stderr)
                sys.exit(1)
        
        # Apply pipeline
        result = apply_pipeline(data, pipeline)
        
        # Output result
        output_json = json.dumps(result, indent=2)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(output_json)
        else:
            print(output_json)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

def validate_pipeline_file(pipeline_path: str) -> bool:
    """
    Validate a pipeline file and return True if valid
    """
    try:
        pipeline = load_pipeline(pipeline_path)
        validation_errors = validate_pipeline(pipeline)
        
        if validation_errors:
            print("Pipeline validation errors:")
            for error in validation_errors:
                print(f"  - {error}")
            return False
        else:
            print(f"Pipeline valid: {len(pipeline)} operations")
            return True
            
    except Exception as e:
        print(f"Error: {e}")
        return False
