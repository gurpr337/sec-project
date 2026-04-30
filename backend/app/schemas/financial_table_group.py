from pydantic import BaseModel

class FinancialTableGroup(BaseModel):
    id: int
    name: str

    class Config:
        from_attributes = True
