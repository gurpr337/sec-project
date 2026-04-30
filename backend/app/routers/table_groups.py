from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from .. import models, schemas
from ..database import get_db
from ..models import Company, TableGroup, DocumentTable, MetricValue, Document
from ..services.table_grouping_service import TableGroupingService

router = APIRouter()

@router.get("/companies/{company_ticker}/table-groups", response_model=List[schemas.TableGroup])
async def get_table_groups(company_ticker: str, db: Session = Depends(get_db)):
    company = db.query(models.Company).filter(models.Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    table_groups = db.query(models.TableGroup).filter(models.TableGroup.company_id == company.id).all()
    return table_groups

@router.get("/companies/{company_ticker}/table-groups/{group_id}/matrix")
async def get_table_group_matrix(
    company_ticker: str,
    group_id: int,
    db: Session = Depends(get_db)
):
    """
    Get parameter matrix for a table group.
    Returns: {
        "parameters": [list of canonical parameter names],
        "time_periods": [list of time periods with document info],
        "matrix": [[values for each parameter x time period]]
    }
    """
    company = db.query(Company).filter(Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_ticker} not found")
    
    group = db.query(TableGroup).filter(
        TableGroup.id == group_id,
        TableGroup.company_id == company.id
    ).first()
    if not group:
        raise HTTPException(status_code=404, detail=f"Table group {group_id} not found")
    
    # Get all metrics in this group
    metrics = db.query(MetricValue).join(DocumentTable).filter(
        DocumentTable.table_group_id == group_id,
        Document.company_id == company.id
    ).order_by(MetricValue.canonical_metric_name, MetricValue.filing_date).all()
    
    if not metrics:
        return {
            "group_name": group.group_name,
            "group_description": group.description,
            "time_periods": [],
            "tables": []
        }
    
    # Build parameter list (unique canonical names)
    parameters = list(set(m.canonical_metric_name for m in metrics))
    parameters.sort()
    
    # Build time periods list (unique dates with document info)
    time_period_data = {}
    for metric in metrics:
        date_key = metric.filing_date.strftime('%Y-%m-%d')
        if date_key not in time_period_data:
            # Get document info
            table = db.query(DocumentTable).filter(DocumentTable.id == metric.source_table_id).first()
            document = table.document if table else None
            
            time_period_data[date_key] = {
                "date": date_key,
                "filing_date": metric.filing_date.isoformat(),
                "form_type": document.form_type if document else "Unknown",
                "document_id": document.id if document else None,
                "year": metric.filing_date.year
            }
    
    time_periods = list(time_period_data.values())
    time_periods.sort(key=lambda x: x['date'])
    
    # Group parameters by table
    table_params = {}
    table_titles = {}
    
    for metric in metrics:
        table_id = metric.source_table_id
        param = metric.canonical_metric_name
        
        if table_id not in table_params:
            table_params[table_id] = set()
            # Get table title - use a more descriptive name
            original_title = metric.source_table.table_title
            if not original_title or original_title == "Untitled Table":
                # Generate title from first parameter's original label
                first_param_label = metric.original_label
                if "balance" in first_param_label.lower():
                    table_titles[table_id] = "Balance Sheet"
                elif "income" in first_param_label.lower() or "revenue" in first_param_label.lower():
                    table_titles[table_id] = "Income Statement"
                elif "cash" in first_param_label.lower():
                    table_titles[table_id] = "Cash Flow Statement"
                elif "equity" in first_param_label.lower():
                    table_titles[table_id] = "Equity Statement"
                elif any(word in first_param_label.lower() for word in ["note", "footnote", "description"]):
                    table_titles[table_id] = "Notes & Disclosures"
                else:
                    # Use first few words of the first parameter
                    words = first_param_label.split()[:3]
                    table_titles[table_id] = " ".join(words)
            else:
                table_titles[table_id] = original_title
        
        table_params[table_id].add(param)
    
    # Build grouped matrix structure
    table_groups_data = []
    
    for table_id, table_parameters in table_params.items():
        table_parameters = sorted(list(table_parameters))
        
        # Build matrix for this table
        table_matrix = []
        for param in table_parameters:
            param_row = []
            for period in time_periods:
                # Find metric value for this parameter and time period
                metric_value = next((
                    m for m in metrics 
                    if m.canonical_metric_name == param and 
                    m.filing_date.strftime('%Y-%m-%d') == period['date'] and
                    m.source_table_id == table_id
                ), None)
                
                if metric_value:
                    param_row.append({
                        "value": metric_value.value,
                        "original_label": metric_value.original_label,
                        "unit_text": metric_value.unit_text,
                        "unit_multiplier": metric_value.unit_multiplier,
                        "table_id": metric_value.source_table_id,
                        "document_url": metric_value.source_table.document.file_url if metric_value.source_table and metric_value.source_table.document else None,
                        "cell_coordinates": metric_value.cell_coordinates,
                        "range_text": metric_value.range_text
                    })
                else:
                    param_row.append(None)
            
            table_matrix.append(param_row)
        
        table_groups_data.append({
            "table_id": table_id,
            "table_title": table_titles[table_id],
            "parameters": table_parameters,
            "matrix": table_matrix
        })
    
    # Sort tables by title for consistent ordering
    table_groups_data.sort(key=lambda x: x['table_title'])
    
    return {
        "group_name": group.group_name,
        "group_description": group.description,
        "time_periods": time_periods,
        "tables": table_groups_data
    }

@router.get("/companies/{company_ticker}/table-groups/{group_id}/tables")
async def get_table_group_tables(
    company_ticker: str,
    group_id: int,
    db: Session = Depends(get_db)
):
    """Get all tables in a table group."""
    company = db.query(Company).filter(Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_ticker} not found")
    
    group = db.query(TableGroup).filter(
        TableGroup.id == group_id,
        TableGroup.company_id == company.id
    ).first()
    if not group:
        raise HTTPException(status_code=404, detail=f"Table group {group_id} not found")
    
    tables = db.query(DocumentTable).join(Document).filter(
        DocumentTable.table_group_id == group_id,
        Document.company_id == company.id
    ).all()
    
    result = []
    for table in tables:
        result.append({
            "id": table.id,
            "table_title": table.table_title,
            "num_rows": table.num_rows,
            "num_cols": table.num_cols,
            "document_id": table.document_id,
            "document_form_type": table.document.form_type if table.document else None,
            "document_filing_date": table.document.filing_date.isoformat() if table.document and table.document.filing_date else None,
            "document_url": table.document.file_url if table.document else None
        })
    
    return {
        "group_name": group.group_name,
        "tables": result
    }

@router.post("/companies/{company_ticker}/create-table-groups")
async def create_table_groups(
    company_ticker: str,
    db: Session = Depends(get_db)
):
    """Create table groups for a company using semantic clustering."""
    company = db.query(Company).filter(Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_ticker} not found")
    
    grouping_service = TableGroupingService()
    groups = grouping_service.create_table_groups(db, company.id)
    
    # Update metric counts
    for group in groups:
        grouping_service.update_group_metrics(db, group.id)
    
    return {
        "message": f"Created {len(groups)} table groups",
        "groups": [
            {
                "id": g.id,
                "group_name": g.group_name,
                "table_count": g.table_count,
                "metric_count": g.metric_count
            }
            for g in groups
        ]
    }

@router.get("/table-groups/{group_id}/similar-parameters")
async def get_similar_parameters(
    group_id: int,
    parameter_name: str = Query(..., description="Parameter name to find similar parameters for"),
    db: Session = Depends(get_db)
):
    """Find parameters similar to the given parameter within a table group."""
    group = db.query(TableGroup).filter(TableGroup.id == group_id).first()
    if not group:
        raise HTTPException(status_code=404, detail=f"Table group {group_id} not found")
    
    # Get all unique parameters in this group
    parameters = db.query(MetricValue.canonical_metric_name, MetricValue.original_label).filter(
        MetricValue.table_group_id == group_id
    ).distinct().all()
    
    # For now, return simple text similarity
    # TODO: Use embedding similarity for better matching
    similar_params = []
    search_terms = parameter_name.lower().split()
    
    for param_name, original_label in parameters:
        if param_name == parameter_name:
            continue
            
        # Check if any search terms appear in parameter name or original label
        param_text = f"{param_name} {original_label or ''}".lower()
        if any(term in param_text for term in search_terms):
            similar_params.append({
                "canonical_name": param_name,
                "original_label": original_label,
                "similarity_score": 0.8  # Placeholder
            })
    
    return {
        "parameter": parameter_name,
        "similar_parameters": similar_params[:10]  # Top 10
    }

@router.get("/{group_id}/matrix")
def get_table_group_matrix(group_id: int, db: Session = Depends(get_db)):
    table_group = db.query(models.TableGroup).filter(models.TableGroup.id == group_id).first()
    if not table_group:
        raise HTTPException(status_code=404, detail="Table group not found")

    # Get all canonical column headers for this group
    column_headers = db.query(models.ColumnHeader).filter(models.ColumnHeader.table_group_id == group_id).all()
    
    # Get all metric values for this group, ordered by section and row index
    metrics = db.query(models.MetricValue).options(
        joinedload(models.MetricValue.source_table).joinedload(models.DocumentTable.document)
    ).filter(models.MetricValue.table_group_id == group_id).order_by(
        models.MetricValue.section_header,
        models.MetricValue.row_index_in_section
    ).all()
    
    parameters = sorted(list(set(m.canonical_metric_name for m in metrics)))
    
    matrix = []
    for param in parameters:
        row = []
        for header in column_headers:
            metric = next((m for m in metrics if m.canonical_metric_name == param and m.column_header_id == header.id), None)
            row.append(metric)
        matrix.append(row)
        
    return {
        "group_name": table_group.group_name,
        "group_description": table_group.description,
        "parameters": parameters,
        "time_periods": [{"date": h.header_text, "form_type": ""} for h in column_headers],
        "matrix": matrix
    }
