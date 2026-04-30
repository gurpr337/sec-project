from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload
from typing import List, Dict
from .. import models, schemas
from ..database import get_db

router = APIRouter()

def build_header_tree(headers: List[models.ColumnHeader]) -> List[schemas.StructuredHeader]:
    header_map = {h.id: schemas.StructuredHeader(**h.__dict__, children=[]) for h in headers}
    root_headers = []
    for header in headers:
        if header.parent_id:
            parent = header_map.get(header.parent_id)
            if parent:
                parent.children.append(header_map[header.id])
        else:
            root_headers.append(header_map[header.id])
    return root_headers

def build_metric_tree(metrics: List[models.FinancialMetric], data_points: List[models.DataPoint]) -> List[schemas.StructuredMetric]:
    metric_map = {m.id: schemas.StructuredMetric(**m.__dict__, children=[], data_points=[]) for m in metrics}
    
    # Associate data points with metrics
    for dp in data_points:
        if dp.metric_id in metric_map:
            metric_map[dp.metric_id].data_points.append(schemas.DataPoint(value=dp.value))

    root_metrics = []
    for metric in metrics:
        if metric.parent_id:
            parent = metric_map.get(metric.parent_id)
            if parent:
                parent.children.append(metric_map[metric.id])
        else:
            root_metrics.append(metric_map[metric.id])
    return root_metrics

@router.get("/financial-tables/{table_id}/structured", response_model=schemas.StructuredFinancialTable)
def get_structured_table(table_id: int, db: Session = Depends(get_db)):
    table = db.query(models.FinancialTable).filter(models.FinancialTable.id == table_id).options(
        joinedload(models.FinancialTable.column_headers),
        joinedload(models.FinancialTable.metrics).joinedload(models.FinancialMetric.data_points)
    ).first()

    if not table:
        raise HTTPException(status_code=404, detail="Financial table not found")

    all_headers = table.column_headers
    all_metrics = table.metrics
    all_data_points = [dp for metric in all_metrics for dp in metric.data_points]

    header_tree = build_header_tree(all_headers)
    metric_tree = build_metric_tree(all_metrics, all_data_points)

    return {
        "id": table.id,
        "title": table.title,
        "headers": header_tree,
        "metrics": metric_tree
    }

@router.get("/groups/{group_id}/evolution")
async def get_table_group_evolution(group_id: int, db: Session = Depends(get_db)):
    """
    Get table evolution data for a table group, with columns grouped by document date.
    Returns data structured for chronological table evolution display.
    """
    try:
        # Get all tables in this group, ordered by document filing date
        tables = db.query(models.FinancialTable).join(models.Document).filter(
            models.FinancialTable.table_group_id == group_id
        ).order_by(models.Document.filing_date).all()

        if not tables:
            raise HTTPException(status_code=404, detail="No tables found in this group")

        # Build evolution data structure
        evolution_data = []

        for table in tables:
            # Get document info
            doc = table.document
            if not doc:
                continue

            # Get all metrics for this table (excluding section headers for display)
            metrics_data = []
            for metric in table.metrics:
                if metric.is_section_header:
                    continue  # Skip section headers in evolution view

                # Get data points for this metric, indexed by column header
                data_points = {}
                for dp in metric.data_points:
                    try:
                        header_name = dp.header.flattened_name if dp.header else f"col_{dp.header_id}"
                        data_points[header_name] = {
                            "value": dp.value,
                            "cell_coordinates": dp.cell_coordinates
                        }
                    except Exception as e:
                        print(f"Error processing data point: {e}")
                        continue

                metrics_data.append({
                    "raw_name": metric.raw_name,
                    "flattened_name": metric.flattened_name,
                    "section": "default",  # Could be enhanced to show parent section
                    "data_points": data_points
                })

            # Sort metrics by their position (would need row_index if we had it)
            metrics_data.sort(key=lambda m: m["flattened_name"])

            evolution_data.append({
                "document_id": doc.id,
                "document_date": doc.filing_date.isoformat(),
                "document_type": doc.form_type,
                "document_url": doc.file_url,
                "table_id": table.id,
                "table_title": table.title,
                "column_headers": [h.flattened_name for h in table.column_headers],
                "metrics": metrics_data
            })

        return {"evolution": evolution_data}

    except Exception as e:
        print(f"Error in get_table_group_evolution: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
