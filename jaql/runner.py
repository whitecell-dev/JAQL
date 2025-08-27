"""
JAQL pipeline runner: enhanced with multi-stage and multi-document support
Enables correlation of data from multiple sources (like ps + free)
"""

import sys
import json
from typing import Any, Dict, List
from pathlib import Path

from .parser import load_pipeline, validate_pipeline
from .pipes import apply_pipeline
from .io import load_stream, normalize_to_records, output_result, load_from_file_or_stdin

try:
    import yaml
    HAS_YAML = True
except ImportError:
    yaml = None
    HAS_YAML = False

def run_pipeline_single_stage(records: List[Dict[str, Any]], pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Run a traditional single-stage pipeline (backward compatibility)
    
    Args:
        records: List of record dictionaries
        pipeline: List of pipe operations
        
    Returns:
        List of transformed records
    """
    return apply_pipeline(records, pipeline)

def run_pipeline_multi_stage(docs: List[Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run a multi-stage pipeline with named stages and cross-document correlation
    
    Args:
        docs: List of documents from multi-document input
        spec: Pipeline specification with 'stages' and optional 'outputs'
        
    Returns:
        Dictionary of stage results or named outputs
    """
    stage_results = {}
    
    # Execute each stage
    for stage_name, stage_config in spec["stages"].items():
        # Get input document index (defaults to 0)
        doc_index = stage_config.get("input", 0)
        
        if doc_index >= len(docs):
            raise ValueError(f"Stage '{stage_name}': input document index {doc_index} out of range (have {len(docs)} docs)")
        
        # Normalize document to records and apply pipes
        records = normalize_to_records(docs[doc_index])
        
        # Get pipeline operations (support both 'pipes' and 'steps' keys for flexibility)
        pipes = stage_config.get("pipes") or stage_config.get("steps", [])
        
        # Apply pipeline operations
        result = apply_pipeline(records, pipes)
        stage_results[stage_name] = result
    
    # Handle outputs specification
    outputs_spec = spec.get("outputs", {})
    if outputs_spec:
        named_outputs = {}
        for output_name, output_config in outputs_spec.items():
            source_stage = output_config["from"]
            if source_stage not in stage_results:
                raise ValueError(f"Output '{output_name}': source stage '{source_stage}' not found")
            named_outputs[output_name] = stage_results[source_stage]
        return named_outputs
    else:
        # Return all stage results if no outputs specified
        return stage_results

def load_pipeline_spec(pipeline_path: str) -> Dict[str, Any]:
    """
    Load pipeline specification from YAML file
    
    Args:
        pipeline_path: Path to pipeline file
        
    Returns:
        Pipeline specification dictionary
    """
    try:
        pipeline_text = Path(pipeline_path).read_text()
    except FileNotFoundError:
        raise ValueError(f"Pipeline file not found: {pipeline_path}")
    
    if HAS_YAML:
        try:
            return yaml.safe_load(pipeline_text)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {pipeline_path}: {e}")
    else:
        try:
            return json.loads(pipeline_text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {pipeline_path}: {e}")

def run_pipeline(pipeline_path: str, input_file: str = None, output_file: str = None, output_format: str = "json") -> None:
    """
    Run a JAQL pipeline on input data (enhanced version)
    
    Args:
        pipeline_path: Path to YAML/JSON pipeline file
        input_file: Path to input file (None for stdin)
        output_file: Path to output file (None for stdout)
        output_format: Output format ("json" or "yaml")
    """
    try:
        # Load pipeline specification
        spec = load_pipeline_spec(pipeline_path)
        
        # Load input data as stream of documents
        input_text = load_from_file_or_stdin(input_file)
        docs = load_stream(input_text)
        
        if not docs:
            raise ValueError("No input documents found")
        
        # Determine pipeline type and execute
        if "stages" in spec:
            # Multi-stage pipeline
            result = run_pipeline_multi_stage(docs, spec)
        else:
            # Traditional single-stage pipeline (backward compatibility)
            if "pipes" not in spec:
                raise ValueError("Pipeline must contain either 'pipes' or 'stages'")
            
            # Use first document for single-stage pipeline
            records = normalize_to_records(docs[0])
            
            # Validate traditional pipeline format
            pipeline = spec["pipes"]
            validation_errors = validate_pipeline(pipeline)
            
            if validation_errors:
                print("Pipeline validation errors:", file=sys.stderr)
                for error in validation_errors:
                    print(f"  - {error}", file=sys.stderr)
                sys.exit(1)
            
            result = run_pipeline_single_stage(records, pipeline)
        
        # Output result
        if output_file:
            with open(output_file, 'w') as f:
                output_result(result, output_format, f)
        else:
            output_result(result, output_format)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

def validate_pipeline_file(pipeline_path: str) -> bool:
    """
    Validate a pipeline file and return True if valid
    """
    try:
        spec = load_pipeline_spec(pipeline_path)
        
        if "stages" in spec:
            # Validate multi-stage pipeline
            if not isinstance(spec["stages"], dict):
                print("Multi-stage pipeline: 'stages' must be a dictionary")
                return False
            
            for stage_name, stage_config in spec["stages"].items():
                if not isinstance(stage_config, dict):
                    print(f"Stage '{stage_name}': must be a dictionary")
                    return False
                
                pipes = stage_config.get("pipes") or stage_config.get("steps", [])
                if not isinstance(pipes, list):
                    print(f"Stage '{stage_name}': 'pipes' or 'steps' must be a list")
                    return False
                
                # Validate pipeline operations
                validation_errors = validate_pipeline(pipes)
                if validation_errors:
                    print(f"Stage '{stage_name}' validation errors:")
                    for error in validation_errors:
                        print(f"  - {error}")
                    return False
            
            print(f"Multi-stage pipeline valid: {len(spec['stages'])} stages")
            return True
        else:
            # Validate traditional single-stage pipeline
            if "pipes" not in spec:
                print("Pipeline must contain either 'pipes' or 'stages'")
                return False
            
            pipeline = spec["pipes"]
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
