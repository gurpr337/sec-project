from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from ..database import Base

class Sigma(Base):
    __tablename__ = 'sigma'
    __table_args__ = {'schema': 'sec_app'}

    id = Column(Integer, primary_key=True, index=True)
    data_point_id = Column(Integer, ForeignKey('sec_app.data_points.id'))

    metric = Column(String)  # flattened_name from financial_metrics
    period_ended = Column(String)  # Parsed period (e.g., "30-Jun 2024")
    value = Column(Float)  # Numeric value
    denomination = Column(String)  # Units (e.g., "millions")
    source_table_name = Column(String)  # Table title
    source_cell = Column(String)  # URL for document viewer

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    data_point = relationship("DataPoint")
