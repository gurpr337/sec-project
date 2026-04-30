from pydantic import BaseModel
from typing import List, Optional

class DataPoint(BaseModel):
    value: Optional[float]

class StructuredMetric(BaseModel):
    id: int
    raw_name: str
    is_section_header: bool
    children: List['StructuredMetric']
    data_points: List[DataPoint]

class StructuredHeader(BaseModel):
    id: int
    raw_name: str
    flattened_name: str
    level: int
    children: List['StructuredHeader']

class StructuredFinancialTable(BaseModel):
    id: int
    title: str
    headers: List[StructuredHeader]
    metrics: List[StructuredMetric]

StructuredMetric.update_forward_refs()
StructuredHeader.update_forward_refs()
