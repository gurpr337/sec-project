from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Text, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base

class Document(Base):
    __tablename__ = 'documents'
    __table_args__ = {'schema': 'sec_app'}

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey('sec_app.companies.id'), nullable=False)
    accession_number = Column(String(50), unique=True, index=True, nullable=False)
    form_type = Column(String(20))
    filing_date = Column(DateTime)
    period_ending = Column(DateTime)
    file_url = Column(String(500))
    file_size = Column(BigInteger)
    is_processed = Column(Boolean, default=False)
    processing_status = Column(String(50), default='pending')
    error_message = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    company = relationship("Company", back_populates="documents")
    financial_tables = relationship("FinancialTable", back_populates="document")
