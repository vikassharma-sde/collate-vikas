import argparse
import sys
from ingestion_game.core import IngestionEngine
from ingestion_game.models import Schema

def main():
    parser = argparse.ArgumentParser(description="The Ingestion Game CLI")
    
    parser.add_argument(
        'input_file', 
        help="Path to the input data file"
    )
    
    parser.add_argument(
        '--keys', 
        type=str, 
        default="id:int,name:str,food:str,type:str",
        help="Comma-separated important keys with types (e.g., 'id:int,name:str')"
    )
    
    parser.add_argument(
        '--hierarchy', 
        type=str, 
        default="A,B,C",
        help="Comma-separated hierarchy of types (e.g., 'A,B,C')"
    )

    args = parser.parse_args()

    try:
        schema = Schema.from_string(args.keys)
    except ValueError as e:
        print(f"Error parsing schema: {e}", file=sys.stderr)
        sys.exit(1)

    hierarchy = [h.strip() for h in args.hierarchy.split(',')]

    engine = IngestionEngine(schema, hierarchy)
    engine.process_file(args.input_file)
    engine.output_results()

if __name__ == "__main__":
    main()
