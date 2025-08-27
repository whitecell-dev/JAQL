# JAQL-lite: JSON + YAML Query Language

A `jq`-like tool that uses YAML pipelines for JSON transformation. Simple, readable, and focused on the most common data processing operations.

## Installation

```bash
pip install jaql-lite
```

Or from source:

```bash
git clone <repository>
cd jaql-lite
pip install -e .
```

## Quick Start

Transform JSON data using YAML pipelines:

```bash
cat examples/input.json | jaql run examples/transform.yaml
```

## Core Operations

JAQL supports three core operations in YAML pipelines:

### `select` - Filter records
```yaml
pipes:
  - select: "age >= 18"           # Keep adults only
  - select: "name == 'Alice'"     # Exact match
  - select: "age >= 18 and active == true"  # Multiple conditions
```

### `project` - Select fields  
```yaml
pipes:
  - project: ["name", "email"]    # Keep only these fields
  - project: ["id"]               # Single field
```

### `derive` - Add computed fields
```yaml
pipes:
  - derive: 
      is_adult: "age >= 18"
      name_length: "len(name)"
      full_name: "first_name + ' ' + last_name"
```

## Examples

### Example 1: Basic filtering and projection

**Input** (`examples/input.json`):
```json
[
  {"name": "Alice", "age": 30, "email": "alice@example.com"},
  {"name": "Bob", "age": 15, "email": "bob@example.com"},
  {"name": "Charlie", "age": 25, "email": "charlie@example.com"}
]
```

**Pipeline** (`examples/transform.yaml`):
```yaml
pipes:
  - select: "age >= 18"
  - derive: {"is_adult": "age >= 18", "name_length": "len(name)"}
  - project: ["name", "email", "is_adult", "name_length"]
```

**Command**:
```bash
cat examples/input.json | jaql run examples/transform.yaml
```

**Output**:
```json
[
  {
    "name": "Alice",
    "email": "alice@example.com", 
    "is_adult": true,
    "name_length": 5
  },
  {
    "name": "Charlie",
    "email": "charlie@example.com",
    "is_adult": true,
    "name_length": 7
  }
]
```

### Example 2: Working with single records

```bash
echo '{"name": "Alice", "age": 30}' | jaql run transform.yaml
```

JAQL automatically handles both single objects and arrays of objects.

## CLI Reference

### Commands

- `jaql run <pipeline.yaml>` - Run pipeline on JSON input
- `jaql validate <pipeline.yaml>` - Validate pipeline syntax

### Options

- `--input`, `-i` - Input JSON file (default: stdin)
- `--output`, `-o` - Output file (default: stdout)

### Examples

```bash
# Process from stdin to stdout
cat data.json | jaql run pipeline.yaml

# Process files
jaql run pipeline.yaml --input data.json --output result.json

# Validate pipeline
jaql validate pipeline.yaml
```

## Expression Language

JAQL uses Python expressions for `select` and `derive` operations:

### Comparison operators
- `==`, `!=`, `<`, `<=`, `>`, `>=`
- `in`, `not in`

### Logical operators  
- `and`, `or`, `not`

### Functions
- `len(field)` - String/array length
- Basic Python built-ins in expressions

### Safety
- Expressions are evaluated safely - invalid expressions return `False`
- No access to file system or dangerous operations
- Malformed data is handled gracefully

## Development

### Running tests
```bash
pip install -e .[dev]
pytest tests/
```

### Project structure
```
jaql-lite/
├── jaql/           # Main package
├── examples/       # Example pipelines and data
├── tests/          # Unit tests
└── README.md       # This file
```

## Philosophy

JAQL focuses on the most common JSON transformation patterns:

1. **Filter** records based on conditions (`select`)
2. **Extract** specific fields (`project`) 
3. **Compute** new fields from existing data (`derive`)

It's designed to be:
- **Simple**: Three operations cover 80% of use cases
- **Readable**: YAML is human-friendly
- **Safe**: No side effects, safe expression evaluation
- **Composable**: Pipes chain naturally

For more advanced features like joins, aggregations, and side effects, see the full AXIOM framework.

## License

MIT License
