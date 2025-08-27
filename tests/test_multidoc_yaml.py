"""
Enhanced JAQL tests: YAML input/output and multi-document support
"""

import pytest
import json
from io import StringIO
from unittest.mock import patch
import sys

# Import the enhanced modules
from jaql.io import load_stream, normalize_to_records, output_result
from jaql.utils import safe_eval
from jaql.pipes import apply_pipeline
from jaql.runner import run_pipeline_multi_stage

# Test fixtures
YAML_MULTI_DOC = """---
- user: root
  command: systemd
  cpu_percent: 2.5
  mem_percent: 1.2
- user: alice  
  command: firefox
  cpu_percent: 15.3
  mem_percent: 8.7
---
type: Mem
total: 8589934592
used: 4294967296
available: 4294967296
"""

SAMPLE_PS_DATA = [
    {"user": "root", "command": "systemd", "cpu_percent": 2.5, "mem_percent": 1.2},
    {"user": "alice", "command": "firefox", "cpu_percent": 15.3, "mem_percent": 8.7},
    {"user": "bob", "command": "vim", "cpu_percent": 0.1, "mem_percent": 0.5}
]

SAMPLE_MEMORY_DATA = {
    "type": "Mem", 
    "total": 8589934592, 
    "used": 4294967296, 
    "available": 4294967296
}

class TestYAMLIO:
    """Test YAML input/output functionality"""
    
    def test_load_stream_yaml_multi_doc(self):
        """Test loading multi-document YAML"""
        docs = load_stream(YAML_MULTI_DOC)
        assert len(docs) == 2
        assert isinstance(docs[0], list)
        assert isinstance(docs[1], dict)
        assert docs[1]["type"] == "Mem"
    
    def test_load_stream_json_fallback(self):
        """Test JSON fallback when YAML parsing fails"""
        json_data = '{"test": "value"}'
        docs = load_stream(json_data)
        assert len(docs) == 1
        assert docs[0] == {"test": "value"}
    
    def test_normalize_to_records(self):
        """Test record normalization"""
        # List of dicts (already normalized)
        records = normalize_to_records(SAMPLE_PS_DATA)
        assert len(records) == 3
        assert all(isinstance(r, dict) for r in records)
        
        # Single dict
        records = normalize_to_records(SAMPLE_MEMORY_DATA)
        assert len(records) == 1
        assert records[0]["type"] == "Mem"
        
        # Primitive value
        records = normalize_to_records("test")
        assert len(records) == 1
        assert records[0] == {"value": "test"}
    
    def test_output_result_yaml(self):
        """Test YAML output formatting"""
        data = {"test": "value", "number": 42}
        output = StringIO()
        
        # Test YAML output
        output_result(data, "yaml", output)
        yaml_output = output.getvalue()
        assert "test: value" in yaml_output
        assert "number: 42" in yaml_output
    
    def test_output_result_json(self):
        """Test JSON output formatting"""
        data = {"test": "value", "number": 42}
        output = StringIO()
        
        # Test JSON output
        output_result(data, "json", output)
        json_output = output.getvalue()
        parsed = json.loads(json_output.strip())
        assert parsed == data

class TestEnhancedExpressions:
    """Test enhanced expression evaluation for JC compatibility"""
    
    def test_hyphenated_keys_with_rec(self):
        """Test accessing hyphenated keys using 'rec' binding"""
        context = {"load-state": "loaded", "cpu-usage": 75.5}
        
        assert safe_eval("rec['load-state'] == 'loaded'", context) is True
        assert safe_eval("rec['cpu-usage'] > 70", context) is True
        assert safe_eval("rec['nonexistent'] == 'test'", context) is False
    
    def test_hyphenated_keys_with_get(self):
        """Test accessing hyphenated keys using 'get' helper"""
        context = {"load-state": "loaded", "normal_key": "value"}
        
        assert safe_eval("get('load-state') == 'loaded'", context) is True
        assert safe_eval("get('nonexistent', 'default') == 'default'", context) is True
        assert safe_eval("get('normal_key') == 'value'", context) is True
    
    def test_mixed_key_access(self):
        """Test mixing normal and hyphenated key access"""
        context = {
            "user": "root",
            "load-state": "loaded", 
            "cpu_percent": 15.5
        }
        
        # Mix normal variables and rec access
        assert safe_eval("user == 'root' and rec['load-state'] == 'loaded'", context) is True
        assert safe_eval("cpu_percent > 10 and get('load-state') == 'loaded'", context) is True

class TestMultiStage:
    """Test multi-stage pipeline functionality"""
    
    def test_multi_stage_basic(self):
        """Test basic multi-stage pipeline execution"""
        docs = [SAMPLE_PS_DATA, SAMPLE_MEMORY_DATA]
        
        spec = {
            "stages": {
                "ps_filter": {
                    "input": 0,
                    "pipes": [
                        {"select": "cpu_percent > 1.0"},
                        {"project": ["user", "command", "cpu_percent"]}
                    ]
                },
                "mem_analysis": {
                    "input": 1,
                    "pipes": [
                        {"derive": {"usage_percent": "(used / total) * 100"}}
                    ]
                }
            }
        }
        
        result = run_pipeline_multi_stage(docs, spec)
        
        assert "ps_filter" in result
        assert "mem_analysis" in result
        assert len(result["ps_filter"]) == 2  # root and alice processes
        assert "usage_percent" in result["mem_analysis"][0]
    
    def test_multi_stage_with_outputs(self):
        """Test multi-stage pipeline with named outputs"""
        docs = [SAMPLE_PS_DATA, SAMPLE_MEMORY_DATA]
        
        spec = {
            "stages": {
                "processes": {
                    "input": 0,
                    "pipes": [{"select": "user == 'alice'"}]
                },
                "memory": {
                    "input": 1,
                    "pipes": [{"project": ["total", "used"]}]
                }
            },
            "outputs": {
                "alice_processes": {"from": "processes"},
                "memory_info": {"from": "memory"}
            }
        }
        
        result = run_pipeline_multi_stage(docs, spec)
        
        assert "alice_processes" in result
        assert "memory_info" in result
        assert "processes" not in result  # Only named outputs
        assert "memory" not in result

class TestJCIntegration:
    """Test integration patterns with jc command output"""
    
    def test_systemctl_like_data(self):
        """Test processing systemctl-like data with hyphenated keys"""
        systemctl_data = [
            {
                "unit": "ssh.service",
                "load": "loaded",
                "active": "active", 
                "sub": "running",
                "load-state": "loaded"
            },
            {
                "unit": "apache2.service",
                "load": "loaded",
                "active": "failed",
                "sub": "failed", 
                "load-state": "loaded"
            }
        ]
        
        pipeline = [
            {"select": "active == 'active' and rec['load-state'] == 'loaded'"},
            {"project": ["unit", "sub"]}
        ]
        
        result = apply_pipeline(systemctl_data, pipeline)
        assert len(result) == 1
        assert result[0]["unit"] == "ssh.service"
    
    def test_docker_inspect_like_data(self):
        """Test processing docker inspect-like nested data"""
        docker_data = [
            {
                "Name": "/webapp",
                "HostConfig": {
                    "Privileged": True,
                    "Memory": 1073741824
                }
            },
            {
                "Name": "/database", 
                "HostConfig": {
                    "Privileged": False,
                    "Memory": 2147483648
                }
            }
        ]
        
        pipeline = [
            {"select": "HostConfig and HostConfig.Privileged == True"},
            {"derive": {"alert": "'PRIVILEGED_CONTAINER'", "container": "Name.lstrip('/')"}}
        ]
        
        result = apply_pipeline(docker_data, pipeline)
        assert len(result) == 1
        assert result[0]["alert"] == "PRIVILEGED_CONTAINER"
        assert result[0]["container"] == "webapp"

class TestCommandLineIntegration:
    """Test full command-line integration scenarios"""
    
    @patch('sys.stdin')
    @patch('sys.stdout')
    def test_yaml_pipe_integration(self, mock_stdout, mock_stdin):
        """Test the full jc --yaml-out | jaql pipeline"""
        # This would be the output from: ps aux | jc --ps --yaml-out
        yaml_input = """---
- user: root
  command: systemd
  cpu_percent: 1.0
- user: alice
  command: firefox  
  cpu_percent: 20.5
"""
        
        mock_stdin.read.return_value = yaml_input
        output_capture = StringIO()
        mock_stdout.write = output_capture.write
        
        # Simulate: jaql run high-cpu.yaml --emit yaml
        # This test would require more mocking of the CLI infrastructure
        # but demonstrates the integration pattern
        
        docs = load_stream(yaml_input)
        pipeline = [
            {"select": "cpu_percent > 10.0"},
            {"project": ["user", "command", "cpu_percent"]}
        ]
        
        result = apply_pipeline(docs[0], pipeline)
        assert len(result) == 1
        assert result[0]["user"] == "alice"

# Integration test helpers
def create_test_pipeline_file(content: str, tmp_path):
    """Helper to create temporary pipeline files for testing"""
    pipeline_file = tmp_path / "test_pipeline.yaml"
    pipeline_file.write_text(content)
    return str(pipeline_file)

def create_test_data_file(content: str, tmp_path):
    """Helper to create temporary data files for testing"""
    data_file = tmp_path / "test_data.yaml"
    data_file.write_text(content)
    return str(data_file)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
