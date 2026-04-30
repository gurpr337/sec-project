from sqlalchemy import Column, Integer, String, Text, JSON, ForeignKey, Float, Boolean
from pgvector.sqlalchemy import Vector
from sqlalchemy.orm import relationship

from ..database import Base

class FinancialTable(Base):
    __tablename__ = 'financial_tables'
    __table_args__ = {'schema': 'sec_app'}

    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(Integer, ForeignKey('sec_app.documents.id'))
    table_group_id = Column(Integer, ForeignKey('sec_app.financial_table_groups.id'), nullable=True, index=True)

    title = Column(String)
    currency = Column(String)
    unit = Column(String)
    embedding = Column(Vector(3072))

    document = relationship("Document", back_populates="financial_tables")
    table_group = relationship("FinancialTableGroup", back_populates="financial_tables")
    column_headers = relationship("ColumnHeader", back_populates="table")
    metrics = relationship("FinancialMetric", back_populates="table")
