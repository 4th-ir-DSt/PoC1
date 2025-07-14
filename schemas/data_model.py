"""Logical data model schemas."""

from pydantic import BaseModel, Field
from typing import List, Optional, Any

class Attribute(BaseModel):
    id: str
    name: str
    type: str
    isPrimaryKey: bool = False
    isForeignKey: bool = False
    classification: Optional[str] = None

class Position(BaseModel):
    x: int
    y: int

class Entity(BaseModel):
    id: str
    name: str
    attributes: List[Attribute]
    position: Optional[Position] = None

class Relationship(BaseModel):
    id: str
    fromEntity: str
    toEntity: str
    type: str
    name: str

class LogicalDataModel(BaseModel):
    id: str
    name: str
    entities: List[Entity]
    relationships: List[Relationship] 