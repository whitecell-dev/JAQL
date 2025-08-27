"""
Tests for JAQL core pipeline operations
"""

import pytest
from jaql.pipes import pipe_select, pipe_project, pipe_derive, apply_pipeline

def test_pipe_select():
    """Test select operation filters records correctly"""
    records = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 15},
        {"name": "Charlie", "age": 25}
    ]
    
    # Test age filter
    result = pipe_select(records, "age >= 18")
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Charlie"
    
    # Test name filter
    result = pipe_select(records, "name == 'Bob'")
    assert len(result) == 1
    assert result[0]["name"] == "Bob"
    
    # Test invalid expression (should return empty)
    result = pipe_select(records, "invalid_field > 0")
    assert len(result) == 0

def test_pipe_project():
    """Test project operation selects fields correctly"""
    records = [
        {"name": "Alice", "age": 30, "email": "alice@example.com", "phone": "123-456"},
        {"name": "Bob", "age": 15, "email": "bob@example.com"}
    ]
    
    # Project name and email only
    result = pipe_project(records, ["name", "email"])
    assert len(result) == 2
    assert result[0] == {"name": "Alice", "email": "alice@example.com"}
    assert result[1] == {"name": "Bob", "email": "bob@example.com"}
    
    # Project non-existent field
    result = pipe_project(records, ["name", "nonexistent"])
    assert len(result) == 2
    assert result[0] == {"name": "Alice"}
    assert result[1] == {"name": "Bob"}

def test_pipe_derive():
    """Test derive operation adds computed fields"""
    records = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 15}
    ]
    
    derivations = {
        "is_adult": "age >= 18",
        "name_length": "len(name)"
    }
    
    result = pipe_derive(records, derivations)
    assert len(result) == 2
    
    # Check Alice (adult)
    alice = result[0]
    assert alice["name"] == "Alice"
    assert alice["age"] == 30
    assert alice["is_adult"] == True
    assert alice["name_length"] == 5
    
    # Check Bob (minor)
    bob = result[1]
    assert bob["name"] == "Bob"
    assert bob["age"] == 15
    assert bob["is_adult"] == False
    assert bob["name_length"] == 3

def test_apply_pipeline():
    """Test complete pipeline execution"""
    data = [
        {"name": "Alice", "age": 30, "email": "alice@example.com"},
        {"name": "Bob", "age": 15, "email": "bob@example.com"},
        {"name": "Charlie", "age": 25, "email": "charlie@example.com"}
    ]
    
    pipeline = [
        {"select": "age >= 18"},
        {"derive": {"is_adult": "age >= 18"}},
        {"project": ["name", "email", "is_adult"]}
    ]
    
    result = apply_pipeline(data, pipeline)
    
    # Should have 2 adults (Alice and Charlie)
    assert len(result) == 2
    
    # Check structure
    for record in result:
        assert set(record.keys()) == {"name", "email", "is_adult"}
        assert record["is_adult"] == True

def test_apply_pipeline_single_record():
    """Test pipeline with single dict input"""
    data = {"name": "Alice", "age": 30, "email": "alice@example.com"}
    
    pipeline = [
        {"derive": {"is_adult": "age >= 18"}},
        {"project": ["name", "is_adult"]}
    ]
    
    result = apply_pipeline(data, pipeline)
    
    assert len(result) == 1
    assert result[0] == {"name": "Alice", "is_adult": True}
