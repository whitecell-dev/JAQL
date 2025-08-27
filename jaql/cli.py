"""
JAQL command-line interface (enhanced)
Supports YAML output and multi-document processing
"""

import argparse
from .runner import run_pipeline, validate_pipeline_file

def main():
    """Main CLI entry point with enhanced options"""
    parser = argparse.ArgumentParser(
        description="JAQL: JSON + YAML Query Language - jq-like tool with YAML pipelines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Traditional single-stage pipeline
  cat data.json | jaql run transform.yaml
  ps aux | jc --ps --yaml-out | jaql run pipeline.yaml
  
  # Multi-document input (YAML)
  { echo '---'; ps aux | jc --ps --yaml-out; echo '---'; free | jc --free --yaml-out; } | jaql run analysis.yaml
  
  # File I/O with YAML output
  jaql run pipeline.yaml --input data.json --output result.yaml --emit yaml
  
  # Pipeline validation
  jaql validate pipeline.yaml
        """.strip()
    )
    
    parser.add_argument("command", choices=["run", "validate"], help="Command to execute")
    parser.add_argument("pipeline", help="YAML/JSON pipeline file")
    parser.add_argument("--input", "-i", help="Input file (default: stdin)")
    parser.add_argument("--output", "-o", help="Output file (default: stdout)")
    parser.add_argument("--emit", choices=["json", "yaml"], default="json", 
                       help="Output format (default: json)")
    
    args = parser.parse_args()
    
    if args.command == "run":
        run_pipeline(args.pipeline, args.input, args.output, args.emit)
    elif args.command == "validate":
        if not validate_pipeline_file(args.pipeline):
            exit(1)

if __name__ == "__main__":
    main()
