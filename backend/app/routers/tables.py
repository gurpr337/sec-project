from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import json

from .. import models, schemas
from ..database import get_db

router = APIRouter()

@router.get("/{table_id}/matrix")
async def get_table_matrix(table_id: int, db: Session = Depends(get_db)):
    """
    Get a parameter matrix for a single table.
    """
    table = db.query(models.DocumentTable).filter(models.DocumentTable.id == table_id).first()
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    metrics = db.query(models.MetricValue).filter(models.MetricValue.source_table_id == table_id).order_by(
        models.MetricValue.canonical_metric_name, models.MetricValue.filing_date
    ).all()

    if not metrics:
        return {
            "table_title": table.table_title,
            "parameters": [],
            "time_periods": [],
            "matrix": []
        }

    # Build parameter list (unique canonical names)
    parameters = sorted(list(set(m.canonical_metric_name for m in metrics)))
    
    # Build time periods list (unique dates with document info)
    time_period_data = {}
    for metric in metrics:
        date_key = metric.filing_date.strftime('%Y-%m-%d')
        if date_key not in time_period_data:
            time_period_data[date_key] = {
                "date": date_key,
                "filing_date": metric.filing_date.isoformat(),
                "form_type": table.document.form_type if table.document else "Unknown",
                "document_id": table.document_id,
            }
    
    time_periods = sorted(list(time_period_data.values()), key=lambda x: x['date'])

    # Build the matrix
    matrix = []
    for param in parameters:
        param_row = []
        for period in time_periods:
            metric_value = next((
                m for m in metrics 
                if m.canonical_metric_name == param and 
                m.filing_date.strftime('%Y-%m-%d') == period['date']
            ), None)
            
            if metric_value:
                param_row.append({
                    "value": metric_value.value,
                    "original_label": metric_value.original_label,
                    "unit_text": metric_value.unit_text,
                    "unit_multiplier": metric_value.unit_multiplier,
                    "table_id": metric_value.source_table_id,
                    "document_url": table.document.file_url if table.document else None,
                    "cell_coordinates": metric_value.cell_coordinates,
                    "range_text": metric_value.range_text
                })
            else:
                param_row.append(None)
        
        matrix.append(param_row)

    return {
        "table_title": table.table_title,
        "parameters": parameters,
        "time_periods": time_periods,
        "matrix": matrix
    }

@router.get("/{table_id}", response_model=schemas.DocumentTable)
async def get_document_table(table_id: int, db: Session = Depends(get_db)):
    """
    Get a specific document table by ID.
    """
    table = db.query(models.DocumentTable).filter(models.DocumentTable.id == table_id).first()
    if not table:
        raise HTTPException(status_code=404, detail="Document table not found")
    return table

@router.get("/document/{document_id}", response_model=List[schemas.DocumentTable])
async def get_document_tables(document_id: int, db: Session = Depends(get_db)):
    """
    Get all tables for a specific document.
    """
    tables = db.query(models.DocumentTable).filter(
        models.DocumentTable.document_id == document_id
    ).order_by(models.DocumentTable.table_index).all()
    return tables
