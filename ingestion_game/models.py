from dataclasses import dataclass
from typing import Dict, Any, Type

@dataclass
class Schema:
    """
    Defines the expected keys and their types.
    """
    fields: Dict[str, Type]

    @classmethod
    def from_string(cls, schema_str: str) -> 'Schema':
        """
        Parses a schema string like 'id:int,name:str,food:str,type:str'.
        """
        type_map = {
            'int': int,
            'str': str,
            'float': float,
            'bool': bool
        }
        
        fields = {}
        parts = schema_str.split(',')
        for part in parts:
            if ':' in part:
                key, type_name = part.split(':', 1)
                key = key.strip()
                type_name = type_name.strip()
                if type_name not in type_map:
                    raise ValueError(f"Unsupported type: {type_name}")
                fields[key] = type_map[type_name]
            else:
                # Default to string if no type specified, though instructions imply explicit types
                fields[part.strip()] = str
        
        return cls(fields=fields)

@dataclass
class Entity:
    """
    Represents a parsed and valid row of data.
    """
    data: Dict[str, Any]
    entity_type: str

    def to_csv_row(self, headers: list[str]) -> str:
        return ",".join(str(self.data.get(h, '')) for h in headers)
