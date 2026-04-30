from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class DocumentBase(BaseModel):
    company_id: int
    accession_number: str
    form_type: str
    filing_date: datetime
    file_url: str

class DocumentCreate(DocumentBase):
    period_ending: Optional[datetime] = None
    file_size: Optional[int] = None

class DocumentUpdate(BaseModel):
    period_ending: Optional[datetime] = None
    file_size: Optional[int] = None
    is_processed: Optional[bool] = None
    processing_status: Optional[str] = None
    error_message: Optional[str] = None

class DocumentResponse(DocumentBase):
    id: int
    period_ending: Optional[datetime] = None
    file_size: Optional[int] = None
    is_processed: bool
    processing_status: str
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True
