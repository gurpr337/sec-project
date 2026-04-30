from sqlalchemy import Column, Integer, String
from pgvector.sqlalchemy import Vector
from sqlalchemy.orm import relationship

from ..database import Base

class CanonicalMetric(Base):
    __tablename__ = 'canonical_metrics'
    __table_args__ = {'schema': 'sec_app'}

    id = Column(Integer, primary_key=True, index=True)
    flattened_name = Column(String, unique=True)
    us_gaap_tag = Column(String, unique=True, nullable=True)
    embedding = Column(Vector(3072))

    financial_metrics = relationship("FinancialMetric", back_populates="canonical_metric")
