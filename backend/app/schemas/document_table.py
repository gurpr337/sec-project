from pydantic import BaseModel
from typing import Optional, Dict, Any

class DocumentTableResponse(BaseModel):
    id: int
    document_id: int
    table_index: int
    table_title: Optional[str] = None
    headers: Optional[Dict[str, Any]] = None
    raw_html: Optional[str] = None
    extracted_data: Optional[Dict[str, Any]] = None
    num_rows: Optional[int] = None
    num_cols: Optional[int] = None
    content_hash: Optional[str] = None
    
    class Config:
        from_attributes = True
