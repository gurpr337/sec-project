from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, Any

# Base schema for a MetricValue
class MetricValueBase(BaseModel):
    canonical_metric_name: str
    value: float
    filing_date: datetime
    original_label: str | None = None
    unit_text: str | None = None
    unit_multiplier: float | None = None

# Schema for creating a new MetricValue (e.g., via API)
class MetricValueCreate(MetricValueBase):
    company_id: int
    source_table_id: int

# Schema for reading/returning a MetricValue from the API
class MetricValueResponse(BaseModel):
    id: int
    company_id: int
    source_table_id: int
    canonical_metric_name: str
    original_label: str
    value: float
    unit_text: Optional[str] = None
    unit_multiplier: Optional[float] = None
    filing_date: datetime
    
    # JS Range API coordinates for highlighting
    range_start_container: Optional[str] = None
    range_start_offset: Optional[int] = None
    range_end_container: Optional[str] = None
    range_end_offset: Optional[int] = None
    range_text: Optional[str] = None
    
    # Cell coordinates for reference
    cell_coordinates: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True
