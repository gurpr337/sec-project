from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from pydantic import BaseModel

from .. import models, schemas
from ..database import get_db
from ..services.pinecone_service import PineconeService
from ..services.embedding_service import EmbeddingService
from ..services.metric_mapping_service import MetricMappingService

router = APIRouter()
pinecone_service = PineconeService()
embedding_service = EmbeddingService()
metric_mapping_service = MetricMappingService()

class LocationData(BaseModel):
    range_start_container_path: str
    range_start_offset: int
    range_end_container_path: str
    range_end_offset: int
    range_text: str
    xpath: str

@router.post("/{metric_value_id}/location", status_code=200)
async def update_metric_location(metric_value_id: int, location_data: LocationData, db: Session = Depends(get_db)):
    metric_value = db.query(models.MetricValue).filter(models.MetricValue.id == metric_value_id).first()
    if not metric_value:
        raise HTTPException(status_code=404, detail="Metric value not found")

    metric_value.range_start_container = location_data.range_start_container_path
    metric_value.range_start_offset = location_data.range_start_offset
    metric_value.range_end_container = location_data.range_end_container_path
    metric_value.range_end_offset = location_data.range_end_offset
    metric_value.range_text = location_data.range_text
    
    if metric_value.cell_coordinates:
        metric_value.cell_coordinates['xpath'] = location_data.xpath
    else:
        metric_value.cell_coordinates = {'xpath': location_data.xpath}

    db.commit()
    return {"status": "success"}

@router.get("/{company_ticker}/list")
async def list_company_metrics(company_ticker: str, db: Session = Depends(get_db)):
    company = db.query(models.Company).filter(models.Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    rows = db.query(models.MetricValue.canonical_metric_name).filter(models.MetricValue.company_id == company.id).distinct().all()
    return sorted([r[0] for r in rows])

@router.get("/{company_ticker}/{canonical_metric_name}", response_model=List[schemas.MetricValue])
async def get_metric_time_series(company_ticker: str, canonical_metric_name: str, db: Session = Depends(get_db)):
    """
    Get time-series data for a specific canonical metric for a given company.
    Returns [] if no data found.
    """
    company = db.query(models.Company).filter(models.Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    metrics = db.query(models.MetricValue).filter(
        models.MetricValue.company_id == company.id,
        models.MetricValue.canonical_metric_name == canonical_metric_name
    ).order_by(models.MetricValue.filing_date.asc()).all()

    return metrics

@router.get("/{company_ticker}/{canonical_metric_name}/with-tables")
async def get_metric_time_series_with_tables(company_ticker: str, canonical_metric_name: str, db: Session = Depends(get_db)):
    """
    Get time-series data for a specific canonical metric with table information including highlight data.
    """
    company = db.query(models.Company).filter(models.Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Join with document_tables and documents to get complete information
    metrics = db.query(
        models.MetricValue,
        models.DocumentTable,
        models.Document
    ).join(
        models.DocumentTable, models.MetricValue.source_table_id == models.DocumentTable.id
    ).join(
        models.Document, models.DocumentTable.document_id == models.Document.id
    ).filter(
        models.MetricValue.company_id == company.id,
        models.MetricValue.canonical_metric_name == canonical_metric_name
    ).order_by(models.MetricValue.filing_date.asc()).all()

    result = []
    for metric, table, document in metrics:
        result.append({
            "id": metric.id,
            "value": metric.value,
            "filing_date": metric.filing_date.isoformat(),
            "original_label": metric.original_label,
            "unit_text": metric.unit_text,
            "unit_multiplier": metric.unit_multiplier,
            "range_start_container": metric.range_start_container,
            "range_start_offset": metric.range_start_offset,
            "range_end_container": metric.range_end_container,
            "range_end_offset": metric.range_end_offset,
            "range_text": metric.range_text,
            "cell_coordinates": metric.cell_coordinates,
            "table_id": table.id,
            "table_title": table.table_title,
            "document_url": document.file_url,
            "form_type": document.form_type,
            "accession_number": document.accession_number
        })

    return result

@router.get("/tables/{table_id}/similar", response_model=List[schemas.DocumentTable])
async def find_similar_tables(table_id: int, top_k: int = 10, db: Session = Depends(get_db)):
    """
    Find tables that are semantically similar to a given table using vector search.
    """
    source_table = db.query(models.DocumentTable).filter(models.DocumentTable.id == table_id).first()
    if not source_table:
        raise HTTPException(status_code=404, detail="Source table not found")

    # Load headers from JSON string to create text for embedding
    try:
        headers_list = json.loads(source_table.headers) if source_table.headers else []
    except json.JSONDecodeError:
        headers_list = []

    text_to_embed = f"Title: {source_table.table_title} Headers: {', '.join(headers_list)}"
    vector = embedding_service.get_embedding(text_to_embed)

    if not vector:
        raise HTTPException(status_code=500, detail="Could not generate embedding for source table")

    # Query Pinecone for similar vectors
    similar_results = pinecone_service.query_similar_tables(vector=vector, top_k=top_k)

    similar_table_ids = [int(match['id']) for match in similar_results if match['id'] != str(table_id)]

    if not similar_table_ids:
        return []

    # Fetch the full table data from PostgreSQL for the similar tables
    similar_tables = db.query(models.DocumentTable).filter(models.DocumentTable.id.in_(similar_table_ids)).all()

    return similar_tables

@router.get("/search")
async def search_metric(
    company_ticker: str = Query(..., description="Ticker symbol, e.g., UNH"),
    query: str = Query(..., description="Metric search text, e.g., 'total revenue'"),
    db: Session = Depends(get_db)
):
    """
    Search for a metric by free-text query and return the closest matching time series.
    1) Try canonical mapping directly from the query text.
    2) If no direct map, fall back to semantic search over Pinecone and pick the best-known mapped metric from top tables.
    """
    company = db.query(models.Company).filter(models.Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Step 1: direct mapping
    canonical = metric_mapping_service.map_label_to_canonical(query)
    if canonical:
        metrics = db.query(models.MetricValue).filter(
            models.MetricValue.company_id == company.id,
            models.MetricValue.canonical_metric_name == canonical
        ).order_by(models.MetricValue.filing_date.asc()).all()
        if metrics:
            return {
                "match_type": "direct",
                "canonical_metric_name": canonical,
                "values": [schemas.MetricValue.model_validate(m) for m in metrics]
            }

    # Step 2: semantic fallback
    qvec = embedding_service.get_embedding(query)
    if not qvec:
        return {"match_type": "none", "canonical_metric_name": None, "values": []}
    matches = pinecone_service.query_similar_tables(vector=qvec, top_k=20)
    table_ids = [int(m["id"]) for m in matches]
    if not table_ids:
        return {"match_type": "none", "canonical_metric_name": None, "values": []}

    tables = db.query(models.DocumentTable).filter(models.DocumentTable.id.in_(table_ids)).all()
    # Scan table rows to find first mapped metric that has values in DB
    import json as _json
    for tbl in tables:
        try:
            rows = _json.loads(tbl.extracted_data) if tbl.extracted_data else []
        except Exception:
            rows = []
        candidate_canonicals = set()
        for row in rows[:10]:
            if not row:
                continue
            mapped = metric_mapping_service.map_label_to_canonical(str(row[0]))
            if mapped:
                candidate_canonicals.add(mapped)
        for cand in candidate_canonicals:
            metrics = db.query(models.MetricValue).filter(
                models.MetricValue.company_id == company.id,
                models.MetricValue.canonical_metric_name == cand
            ).order_by(models.MetricValue.filing_date.asc()).all()
            if metrics:
                return {
                    "match_type": "semantic",
                    "canonical_metric_name": cand,
                    "values": [schemas.MetricValue.model_validate(m) for m in metrics]
                }

    return {"match_type": "none", "canonical_metric_name": None, "values": []}
