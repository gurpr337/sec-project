from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class CompanyCreate(BaseModel):
    ticker: str
    name: str

class CompanyUpdate(BaseModel):
    name: Optional[str] = None

class CompanyResponse(BaseModel):
    id: int
    ticker: str
    name: str
    created_at: datetime
    
    class Config:
        from_attributes = True
