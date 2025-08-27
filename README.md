# JAQL: JSON + YAML Query Language

A `jq`-like tool using YAML pipelines for JSON/YAML transformation, now with enhanced support for **JC integration** and multi-document processing.

## Quick Start

### Basic Usage
```bash
# Traditional JSON processing
cat data.json | jaql run pipeline.yaml

# NEW: YAML input/output (perfect for jc integration)
ps aux | jc --ps --yaml-out | jaql run high-cpu.yaml --emit yaml
```

### JC Integration Examples

JAQL now seamlessly integrates with [jc](https://github.com/kellyjonbrazil/jc) for powerful system analysis:

```bash
# Analyze running services
systemctl list-units --type=service --state=running \
| jc --systemctl --yaml-out \
| jaql run running-services.yaml --emit yaml

# Find high-CPU processes  
ps aux | jc --ps --yaml-out | jaql run high-cpu.yaml --emit yaml

# Security audit: check for privileged containers
docker inspect $(docker ps -q) \
| jc --docker-inspect --yaml-out \
| jaql run security-check.yaml --emit yaml
```

## Pipeline Format

### Single-Stage Pipeline (Traditional)
```yaml
pipes:
  - select: "cpu_percent > 5.0"
  - project: ["user", "command", "cpu_percent"]
  - derive: { alert_level: "'HIGH' if cpu_percent > 10 else 'MEDIUM'" }
```

### Multi-Stage Pipeline (NEW)
Perfect for correlating data from multiple sources:

```yaml
stages:
  processes:
    input: 0  # First YAML document
    pipes:
      - select: "cpu_percent > 1.0"
      - project: ["user", "command", "cpu_percent"]
  
  memory:
    input: 1  # Second YAML document  
    pipes:
      - derive: { usage_percent: "(used / total) * 100" }

outputs:
  high_cpu_processes: { from: processes }
  memory_status: { from: memory }
```

Usage with multi-document input:
```bash
{
  echo '---'
  ps aux | jc --ps --yaml-out
  echo '---' 
  free -b | jc --free --yaml-out
} | jaql run system-analysis.yaml --emit yaml
```

## Operations

### Select (σ)
Filter records using Python expressions:
```yaml
- select: "age >= 18"
- select: "status == 'active'"
- select: "rec['load-state'] == 'loaded'"  # For hyphenated keys from jc
```

### Project (π) 
Select specific fields:
```yaml
- project: ["name", "age"]
- project: ["user", "command", "cpu_percent"]
```

### Derive
Add computed fields:
```yaml
- derive:
    full_name: "first_name + ' ' + last_name"
    cpu_alert: "'HIGH' if cpu_percent > 90 else 'OK'"
```

## Expression Features

### Enhanced Key Access
JAQL now handles hyphenated keys from jc output elegantly:

```yaml
# These are equivalent for accessing "load-state" key:
- select: "rec['load-state'] == 'loaded'"
- select: "get('load-state') == 'loaded'"
```

### Built-in Functions
```python
len(name)           # String/list length
str(age)            # Type conversion  
int(score)          # Integer conversion
max(values)         # Maximum value
min(values)         # Minimum value
```

## Real-World Examples

### System Security Audit
```bash
# Check for services that failed to start
systemctl list-units --all --type=service \
| jc --systemctl --yaml-out \
| jaql run failed-services.yaml --emit yaml
```

**failed-services.yaml:**
```yaml
pipes:
  - select: "active == 'failed' and rec['load-state'] == 'loaded'"
  - project: ["unit", "active", "sub", "description"]
  - derive: 
      priority: "'HIGH' if 'ssh' in unit or 'network' in unit else 'MEDIUM'"
```

### Resource Monitoring
```bash
# Identify resource-hungry processes
ps aux | jc --ps --yaml-out | jaql run resource-monitor.yaml --emit yaml
```

**resource-monitor.yaml:**
```yaml
pipes:
  - select: "cpu_percent > 5.0 or mem_percent > 10.0"
  - derive:
      resource_score: "cpu_percent + (mem_percent * 0.5)"
      alert_type: "'CPU' if cpu_percent > mem_percent else 'MEMORY'"
  - project: ["user", "command", "cpu_percent", "mem_percent", "resource_score", "alert_type"]
```

### Generate Monitoring Rules (Meta-Programming!)
```bash
# Auto-generate monitoring rules from current system state
ps aux | jc --ps --yaml-out | jaql run generate-rules.yaml --emit yaml
```

**generate-rules.yaml:**
```yaml
pipes:
  - select: "user == 'root' and cpu_percent > 0"
  - derive:
      rule_name: "'monitor_' + command.replace('/', '_')"
      rule_condition: "'user == \"root\" and command.startswith(\"' + command + '\")'"
      rule_action: "'alert(\"Root process: ' + command + ' using ' + str(cpu_percent) + '% CPU\")'"
  - project: ["rule_name", "rule_condition", "rule_action"]
```

## CLI Options

```bash
jaql run pipeline.yaml [options]

Options:
  --input, -i FILE    Input file (default: stdin)
  --output, -o FILE   Output file (default: stdout)  
  --emit FORMAT       Output format: json|yaml (default: json)

jaql validate pipeline.yaml    # Validate pipeline syntax
```

## Installation

```bash
pip install pyyaml>=6.0
```

For development:
```bash
pip install -r requirements.txt
pytest tests/
```

## Why JAQL + JC?

This combination creates a powerful **unified analysis workflow**:

1. **JC extracts** structured data from unstructured command output
2. **JAQL processes** that data with declarative pipelines  
3. **YAML everywhere** - no format conversion friction
4. **Multi-document support** enables data correlation across commands
5. **Human-readable** pipelines that are also LLM-friendly

Perfect for system administration, security auditing, infrastructure monitoring, and compliance reporting.
