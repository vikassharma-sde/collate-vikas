#!/usr/bin/env python3
"""
ingestion_game CLI

A small CLI tool that ingests lines of key=value pairs (CSV-like per row),
filters rows based on an "important keys" schema with datatypes, and outputs
processed rows ordered by entity hierarchy (phases).

Usage examples:
    python ingestion_game.py --input input.txt --keys id:int,name:str,food:str,type:str --hierarchy A,B,C

The script accepts:
    --input / -i : input file path (default: stdin)
    --keys / -k  : comma-separated list of important keys with datatypes (e.g. id:int,name:str)
    --hierarchy / -H : comma-separated ordered list of entity types (e.g. A,B,C)
    --type-field : name of the key that contains the entity type (default: type)
    --delimiter  : delimiter for key=value pairs (default: ,)
    --output-keys : optional comma-separated output order for header (defaults to order provided in --keys)

Design notes:
- Rows missing an important key or with a value that doesn't match the declared datatype are skipped.
- Rows whose entity-type value is not in the provided hierarchy are skipped.
- Entities are emitted in phases: all entities of hierarchy[0], then hierarchy[1], etc.
- Within each phase, input order is preserved.
"""
import sys
import argparse
import csv
from typing import Dict, List, Tuple, Callable, Optional


def parse_schema(schema_str: str) -> List[Tuple[str, str]]:
    """Parse schema like 'id:int,name:str' into list of (key, type_str)."""
    items = []
    for part in schema_str.split(','):
        if not part.strip():
            continue
        if ':' not in part:
            raise ValueError(f"Invalid schema part '{part}'. Expected key:type.")
        key, type_name = part.split(':', 1)
        items.append((key.strip(), type_name.strip()))
    return items


def type_validator(type_name: str) -> Callable[[str], bool]:
    """Return a validator function for basic types."""
    if type_name == 'int':
        def is_int(val: str) -> bool:
            try:
                int(val)
                return True
            except Exception:
                return False
        return is_int
    elif type_name == 'str':
        def is_str(val: str) -> bool:
            return isinstance(val, str) and val != ''
        return is_str
    else:
        # For unknown types just accept any non-empty string
        def is_any(val: str) -> bool:
            return val != ''
        return is_any


def parse_row(line: str, delimiter: str = ',') -> Dict[str, str]:
    """Parse a line like 'a=1,b=2' into dict{'a':'1','b':'2'}.
    It tolerates spaces. If a pair doesn't have '=', it's ignored.
    """
    d = {}
    parts = [p for p in line.strip().split(delimiter) if p != '']
    for p in parts:
        if '=' not in p:
            continue
        k, v = p.split('=', 1)
        d[k.strip()] = v.strip()
    return d


def validate_row(row: Dict[str, str], schema: List[Tuple[str, str]], validators: Dict[str, Callable[[str], bool]]) -> bool:
    """Return True if row contains all schema keys and each value passes its validator."""
    for key, _ in schema:
        if key not in row:
            return False
        val = row[key]
        # empty values considered invalid
        if val == '':
            return False
        validator = validators.get(key)
        if validator and not validator(val):
            return False
    return True


def run_ingest(lines: List[str],
               schema: List[Tuple[str, str]],
               hierarchy: List[str],
               type_field: str = 'type',
               delimiter: str = ',') -> Tuple[List[str], List[List[str]]]:
    """Process lines and return (header, rows) where rows are lists of values in header order."""
    validators = {k: type_validator(t) for k, t in schema}
    header_keys = [k for k, _ in schema]

    # collect rows by type preserving input order
    by_type = {t: [] for t in hierarchy}

    for line in lines:
        if not line.strip():
            continue
        row = parse_row(line, delimiter=delimiter)
        # If type field missing or type not in hierarchy => skip
        if type_field not in row:
            continue
        typ = row[type_field]
        if typ not in hierarchy:
            continue
        # Validate row contains schema keys and types
        if not validate_row(row, schema, validators):
            continue
        # Append values in header order
        values = [row.get(k, '') for k in header_keys]
        by_type[typ].append(values)

    # Now emit rows in hierarchy order
    out_rows = []
    for t in hierarchy:
        out_rows.extend(by_type.get(t, []))

    return header_keys, out_rows


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog='ingestion_game', description='Ingest lines of key=value pairs honoring schema and hierarchy.')
    parser.add_argument('-i', '--input', type=str, default='-',
                        help='Input file path (default: stdin)')
    parser.add_argument('-k', '--keys', type=str, required=True,
                        help="""Important keys with datatypes, comma-separated. Example: id:int,name:str,food:str,type:str""")
    parser.add_argument('-H', '--hierarchy', type=str, required=True,
                        help='Comma-separated ordered list of entity types: e.g. A,B,C')
    parser.add_argument('--type-field', type=str, default='type', help="Name of the field representing entity type (default: 'type')")
    parser.add_argument('--delimiter', type=str, default=',', help='Delimiter used between key=value pairs (default: ,)')
    parser.add_argument('--output-keys', type=str, default=None, help='Comma-separated header order for output (defaults to order in --keys)')

    args = parser.parse_args(argv)

    try:
        schema = parse_schema(args.keys)
    except ValueError as e:
        print(f"Error parsing schema: {e}", file=sys.stderr)
        return 2

    hierarchy = [h for h in (args.hierarchy.split(',')) if h != '']

    if args.input == '-':
        lines = [l.rstrip('\n') for l in sys.stdin.readlines()]
    else:
        try:
            with open(args.input, 'r', encoding='utf-8') as fh:
                lines = [l.rstrip('\n') for l in fh.readlines()]
        except FileNotFoundError:
            print(f"Input file not found: {args.input}", file=sys.stderr)
            return 2

    header_keys, rows = run_ingest(lines, schema, hierarchy, type_field=args.type_field, delimiter=args.delimiter)

    # possibly reorder header if output-keys provided
    if args.output_keys:
        out_header = [k.strip() for k in args.output_keys.split(',') if k.strip()]
    else:
        out_header = header_keys

    writer = csv.writer(sys.stdout, lineterminator='\n')
    writer.writerow(out_header)
    for r in rows:
        # Map r which was in header_keys order to output order
        if out_header == header_keys:
            writer.writerow(r)
        else:
            mapping = {k: v for k, v in zip(header_keys, r)}
            writer.writerow([mapping.get(k, '') for k in out_header])

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
