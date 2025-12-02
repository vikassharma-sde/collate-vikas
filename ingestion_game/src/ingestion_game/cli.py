#!/usr/bin/env python3
"""
Simple ingestion_game CLI

Usage:
    python ingestion_game.py --input input.txt \
        --keys id:int,name:str,food:str,type:str --hierarchy A,B,C

Behavior (simplified):
- Read lines of comma-separated key=value pairs.
- Keep only rows that contain all important keys and whose values match types.
- Group output by the provided hierarchy order (A then B then C), preserving input order within each group.
- Print CSV to stdout with header in the order given by --keys.
"""
import sys
import argparse
import csv

def parse_schema(s):
    out = []
    for part in s.split(','):
        part = part.strip()
        if not part: continue
        if ':' not in part:
            raise SystemExit(f"Schema parts must be key:type, got: {part}")
        k, t = part.split(':', 1)
        out.append((k.strip(), t.strip()))
    return out

def is_valid(val, t):
    if val == '':
        return False
    if t == 'int':
        try:
            int(val); return True
        except Exception:
            return False
    return True

def parse_line(line, delim=','):
    d = {}
    for part in [p.strip() for p in line.strip().split(delim) if p.strip()]:
        if '=' not in part:
            continue
        k, v = part.split('=', 1)
        d[k.strip()] = v.strip()
    return d

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', '-i', default='-', help='input file (default stdin)')
    p.add_argument('--keys', '-k', required=True, help='comma-separated key:type pairs')
    p.add_argument('--hierarchy', '-H', required=True, help='comma-separated entity types, ordered')
    p.add_argument('--type-field', default='type', help='field name for entity type (default: type)')
    args = p.parse_args()

    schema = parse_schema(args.keys)          # list of (key, type)
    keys_order = [k for k, _ in schema]
    types_order = [t for t in (args.hierarchy.split(',')) if t != '']

    # read input lines
    if args.input == '-':
        lines = [l.rstrip('\n') for l in sys.stdin]
    else:
        try:
            with open(args.input, 'r', encoding='utf-8') as fh:
                lines = [l.rstrip('\n') for l in fh]
        except FileNotFoundError:
            raise SystemExit(f"Input file not found: {args.input}")

    # collect valid rows grouped by type (preserve input order)
    grouped = {t: [] for t in types_order}
    for line in lines:
        if not line.strip(): continue
        row = parse_line(line)
        if args.type_field not in row:            # no type field => skip
            continue
        etype = row[args.type_field]
        if etype not in grouped:                  # not in provided hierarchy => skip
            continue
        # validate presence and types for every important key
        ok = True
        for k, t in schema:
            if k not in row or not is_valid(row[k], t):
                ok = False
                break
        if not ok:
            continue
        # collect values in the requested header order
        grouped[etype].append([row.get(k, '') for k in keys_order])

    # emit CSV to stdout in hierarchy order
    w = csv.writer(sys.stdout, lineterminator='\n')
    w.writerow(keys_order)
    for et in types_order:
        for vals in grouped.get(et, []):
            w.writerow(vals)

if __name__ == '__main__':
    main()
