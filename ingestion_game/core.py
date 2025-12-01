import sys
from typing import List, Optional, Dict, Any
from ingestion_game.models import Schema, Entity

class IngestionEngine:
    def __init__(self, schema: Schema, hierarchy: List[str]):
        self.schema = schema
        self.hierarchy = hierarchy
        self.hierarchy_map = {val: idx for idx, val in enumerate(hierarchy)}
        self.buffer: List[Entity] = []

    def parse_line(self, line: str) -> Optional[Dict[str, str]]:
        """
        Parses a raw line 'key1=val1,key2=val2' into a dictionary.
        Returns None if parsing fails fundamentally.
        """
        line = line.strip()
        if not line:
            return None
        
        raw_data = {}
        # Split by comma, but a robust solution might need to handle escaped commas.
        # Given the simple problem statement, simple split is likely sufficient.
        parts = line.split(',')
        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                raw_data[key.strip()] = value.strip()
        return raw_data

    def validate_and_convert(self, raw_data: Dict[str, str]) -> Optional[Entity]:
        """
        Validates the raw data against the schema.
        Returns an Entity if valid, None otherwise.
        """
        converted_data = {}
        
        # Check all important keys from schema are present
        for key, expected_type in self.schema.fields.items():
            if key not in raw_data:
                # Rule: "All important keys are always going to be present"
                # If missing, it's a malformed row or "noise" row that doesn't fit criteria
                return None
            
            raw_val = raw_data[key]
            try:
                if expected_type == int:
                    converted_data[key] = int(raw_val)
                elif expected_type == float:
                    converted_data[key] = float(raw_val)
                else:
                    converted_data[key] = str(raw_val)
            except ValueError:
                # Type mismatch
                return None

        entity_type = converted_data.get('type')
        if not isinstance(entity_type, str):
             # 'type' must be present as per requirements usually, 
             # and we treat it as the grouping key.
             return None
             
        return Entity(data=converted_data, entity_type=entity_type)

    def process_file(self, file_path: str):
        """
        Reads file, parses, validates, and buffers entities.
        """
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    raw_data = self.parse_line(line)
                    if raw_data:
                        entity = self.validate_and_convert(raw_data)
                        if entity:
                            self.buffer.append(entity)
        except FileNotFoundError:
            print(f"Error: File {file_path} not found.", file=sys.stderr)
            sys.exit(1)

    def get_sorted_entities(self) -> List[Entity]:
        """
        Sorts buffered entities based on hierarchy.
        """
        # Sort key: 
        # 1. Hierarchy Index (A before B)
        # 2. Original 'type' string (fallback for types not in hierarchy, if allowed?)
        #    The prompt implies strict hierarchy compliance for processing.
        #    "we cannot process any Entity of type B until we have processed all Entities of type A"
        
        def sort_key(e: Entity):
            t = e.entity_type
            # Get index from hierarchy map, default to infinity if not in hierarchy (processed last or never?)
            # Assuming inputs fit the hierarchy provided.
            idx = self.hierarchy_map.get(t, float('inf'))
            return idx

        # Filter entities that are not in the hierarchy?
        # "Entities follow the hierarchy A -> B -> C". 
        # If a type 'D' appears, it's undefined behavior. We'll include it at the end or filter.
        # Let's assume we keep them but sort them last.
        
        return sorted(self.buffer, key=sort_key)

    def output_results(self):
        """
        Prints the results to stdout.
        """
        headers = list(self.schema.fields.keys())
        
        # Print CSV Header
        print(",".join(headers))
        
        sorted_entities = self.get_sorted_entities()
        
        for entity in sorted_entities:
            # Only output entities that are part of the defined hierarchy?
            # The rule A->B implies an ordering. If a type is unknown, we might still output it?
            # For strict adherence to "process... until we have processed all...", 
            # usually implies specific set. We will print all valid ones sorted.
            print(entity.to_csv_row(headers))
