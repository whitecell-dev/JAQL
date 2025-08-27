"""
JAQL command-line interface
"""

import argparse
from .runner import run_pipeline, validate_pipeline_file

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="JAQL: JSON + YAML Query Language - jq-like tool with YAML pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  cat data.json | jaql run transform.yaml
  jaql run pipeline.yaml --input data.json --output result.json
  jaql validate pipeline.yaml
        """.strip()
    )
    
    parser.add_argument("command", choices=["run", "validate"], help="Command to execute")
    parser.add_argument("pipeline", help="YAML pipeline file")
    parser.add_argument("--input", "-i", help="Input JSON file (default: stdin)")
    parser.add_argument("--output", "-o", help="Output file (default: stdout)")
    
    args = parser.parse_args()
    
    if args.command == "run":
        run_pipeline(args.pipeline, args.input, args.output)
    elif args.command == "validate":
        if not validate_pipeline_file(args.pipeline):
            exit(1)

if __name__ == "__main__":
    main()
