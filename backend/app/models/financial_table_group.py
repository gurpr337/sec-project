from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from ..database import Base

class FinancialTableGroup(Base):
    __tablename__ = 'financial_table_groups'
    __table_args__ = {'schema': 'sec_app'}

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)

    financial_tables = relationship("FinancialTable", back_populates="table_group")
