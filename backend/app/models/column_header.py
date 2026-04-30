from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from ..database import Base

class ColumnHeader(Base):
    __tablename__ = 'column_headers'
    __table_args__ = {'schema': 'sec_app'}

    id = Column(Integer, primary_key=True, index=True)
    table_id = Column(Integer, ForeignKey('sec_app.financial_tables.id'))
    parent_id = Column(Integer, ForeignKey('sec_app.column_headers.id'))
    
    raw_name = Column(String)
    flattened_name = Column(String)
    level = Column(Integer)

    table = relationship("FinancialTable", back_populates="column_headers")
    parent = relationship("ColumnHeader", remote_side=[id])
    data_points = relationship("DataPoint", back_populates="header")
