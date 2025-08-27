"""
JAQL I/O utilities: YAML/JSON stream loading and output formatting
Enables jc --yaml-out | jaql workflow with multi-document support
"""

import json
from typing import List, Any, Union, TextIO
import sys

try:
    import yaml
    HAS_YAML = True
except ImportError:
    yaml = None
    HAS_YAML = False

def load_stream(text: str) -> List[Any]:
    """
    Load a stream of documents, preferring YAML multi-doc, falling back to JSON
    
    Returns:
        List of documents. YAML multi-doc → [doc0, doc1, ...], single → [doc]
    """
    if not text.strip():
        return []
    
    # Try YAML first (handles multi-doc and single doc)
    if HAS_YAML:
        try:
            docs = list(yaml.safe_load_all(text))
            # Filter out None documents from empty YAML docs
            docs = [doc for doc in docs if doc is not None]
            if len(docs) > 0:
                return docs
        except Exception:
            pass
    
    # Fall back to JSON
    try:
        return [json.loads(text)]
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON input: {e}")

def normalize_to_records(doc: Any) -> List[dict]:
    """
    Normalize a document to a list of record dictionaries for pipeline processing
    
    Args:
        doc: Document (dict, list of dicts, or other)
        
    Returns:
        List of record dictionaries
    """
    if isinstance(doc, list):
        # If it's a list of dicts, return as-is
        if all(isinstance(item, dict) for item in doc):
            return doc
        # If it's a mixed list, wrap each item
        return [{"value": item} if not isinstance(item, dict) else item for item in doc]
    elif isinstance(doc, dict):
        return [doc]
    else:
        # Wrap primitive values
        return [{"value": doc}]

def output_result(result: Any, output_format: str = "json", file: TextIO = None) -> None:
    """
    Output result in specified format to file or stdout
    
    Args:
        result: Data to output
        output_format: "json" or "yaml"
        file: Output file (defaults to stdout)
    """
    if file is None:
        file = sys.stdout
    
    if output_format == "yaml" and HAS_YAML:
        # Use safe_dump with clean formatting
        yaml.safe_dump(result, file, 
                      default_flow_style=False, 
                      sort_keys=False, 
                      indent=2)
    else:
        # Default to JSON
        json.dump(result, file, indent=2)
        file.write('\n')  # Add newline for clean terminal output

def load_from_file_or_stdin(input_file: str = None) -> str:
    """
    Load text content from file or stdin
    
    Args:
        input_file: Path to input file (None for stdin)
        
    Returns:
        Text content
    """
    if input_file:
        try:
            with open(input_file, 'r') as f:
                return f.read()
        except FileNotFoundError:
            raise ValueError(f"Input file not found: {input_file}")
        except Exception as e:
            raise ValueError(f"Error reading {input_file}: {e}")
    else:
        try:
            return sys.stdin.read()
        except Exception as e:
            raise ValueError(f"Error reading from stdin: {e}")
