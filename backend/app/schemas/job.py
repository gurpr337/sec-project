from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime

class ExtractionJobBase(BaseModel):
    company_id: int
    status: str
    job_type: str
    job_metadata: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

class ExtractionJobCreate(ExtractionJobBase):
    pass

class ExtractionJob(ExtractionJobBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        orm_mode = True
