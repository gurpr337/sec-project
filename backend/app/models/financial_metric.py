from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, JSON
from sqlalchemy.orm import relationship

from ..database import Base

class FinancialMetric(Base):
    __tablename__ = 'financial_metrics'
    __table_args__ = {'schema': 'sec_app'}

    id = Column(Integer, primary_key=True, index=True)
    table_id = Column(Integer, ForeignKey('sec_app.financial_tables.id'))
    parent_id = Column(Integer, ForeignKey('sec_app.financial_metrics.id'))
    canonical_metric_id = Column(Integer, ForeignKey('sec_app.canonical_metrics.id'))

    raw_name = Column(String)
    flattened_name = Column(String)  # Hierarchical name like "Revenues - Services"
    is_section_header = Column(Boolean)
    cell_coordinates = Column(JSON)  # Stores {"row": 14, "col": 2} for document viewer

    table = relationship("FinancialTable", back_populates="metrics")
    parent = relationship("FinancialMetric", remote_side=[id])
    canonical_metric = relationship("CanonicalMetric", back_populates="financial_metrics")
    data_points = relationship("DataPoint", back_populates="metric")
