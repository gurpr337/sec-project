from sqlalchemy import Column, Integer, Float, ForeignKey, JSON
from sqlalchemy.orm import relationship

from ..database import Base

class DataPoint(Base):
    __tablename__ = 'data_points'
    __table_args__ = {'schema': 'sec_app'}

    id = Column(Integer, primary_key=True, index=True)
    metric_id = Column(Integer, ForeignKey('sec_app.financial_metrics.id'))
    header_id = Column(Integer, ForeignKey('sec_app.column_headers.id'))
    value = Column(Float)
    cell_coordinates = Column(JSON)  # Stores {"row": X, "col": Y} for document viewer

    metric = relationship("FinancialMetric", back_populates="data_points")
    header = relationship("ColumnHeader", back_populates="data_points")
