"""
Tests for JAQL pipeline runner
"""

import pytest
import tempfile
import json
import os
from jaql.runner import validate_pipeline_file
from jaql.parser import load_pipeline

def test_load_valid_pipeline():
    """Test loading a valid pipeline"""
    pipeline_yaml = """
pipes:
  - select: "age >= 18"
  - project: ["name", "email"]
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(pipeline_yaml)
        f.flush()
        
        try:
            pipeline = load_pipeline(f.name)
            assert len(pipeline) == 2
            assert pipeline[0] == {"select": "age >= 18"}
            assert pipeline[1] == {"project": ["name", "email"]}
        finally:
            os.unlink(f.name)

def test_load_invalid_pipeline():
    """Test loading an invalid pipeline"""
    invalid_yaml = """
pipes:
  - invalid_operation: "test"
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(invalid_yaml)
        f.flush()
        
        try:
            with pytest.raises(ValueError, match="unknown operations"):
                load_pipeline(f.name)
        finally:
            os.unlink(f.name)

def test_validate_pipeline_file():
    """Test pipeline validation"""
    valid_yaml = """
pipes:
  - select: "age >= 18"
  - derive: {"is_adult": "age >= 18"}
  - project: ["name", "email", "is_adult"]
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(valid_yaml)
        f.flush()
        
        try:
            assert validate_pipeline_file(f.name) == True
        finally:
            os.unlink(f.name)
