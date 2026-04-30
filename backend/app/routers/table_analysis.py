"""
API endpoints for table analysis functionality
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from ..database import get_db
from ..services.table_analysis_manager import TableAnalysisManager

router = APIRouter()

# Global analysis manager instance
analysis_manager = TableAnalysisManager()

@router.post("/analyze-unh", response_model=Dict[str, Any])
async def analyze_unh_documents(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Analyze all tables in all UNH 10-K documents using cloned Type 1B logic.
    This runs in the background and stores results in document_table_analysis table.
    """
    try:
        # Run analysis in background
        background_tasks.add_task(run_analysis_async, db)

        return {
            "message": "UNH table analysis started in background",
            "status": "running",
            "description": "Analyzing all tables in 20 UNH 10-K documents using cloned Type 1B extraction logic"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start analysis: {str(e)}")

def run_analysis_async(db: Session):
    """Run the analysis asynchronously"""
    try:
        print("Starting UNH table analysis...")
        results = analysis_manager.analyze_unh_documents(db)
        print(f"Analysis complete: {results}")
    except Exception as e:
        print(f"Analysis failed: {e}")
    finally:
        db.close()

@router.get("/summary", response_model=Dict[str, Any])
async def get_analysis_summary(db: Session = Depends(get_db)):
    """
    Get summary of all table analysis results
    """
    try:
        summary = analysis_manager.get_analysis_summary(db)
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get summary: {str(e)}")

@router.get("/documents/{document_id}/tables", response_model=List[Dict])
async def get_document_tables(document_id: int, db: Session = Depends(get_db)):
    """
    Get all analyzed tables for a specific document
    """
    try:
        tables = analysis_manager.get_document_tables(db, document_id)
        return tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get document tables: {str(e)}")

@router.get("/tables/{table_id}", response_model=Dict)
async def get_table_by_id(table_id: int, db: Session = Depends(get_db)):
    """
    Get a specific analyzed table by ID
    """
    try:
        table = analysis_manager.get_table_by_id(db, table_id)
        if not table:
            raise HTTPException(status_code=404, detail="Table not found")
        return table
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get table: {str(e)}")

@router.get("/unh-documents", response_model=List[Dict])
async def get_unh_documents_with_table_counts(db: Session = Depends(get_db)):
    """
    Get all UNH 10-K documents with their table analysis counts
    """
    try:
        # Get UNH company
        from ..models.company import Company
        unh = db.query(Company).filter(Company.ticker == 'UNH').first()
        if not unh:
            return []

        # Get documents with table counts
        from sqlalchemy import text
        results = db.execute(text("""
            SELECT
                d.id,
                d.accession_number,
                d.filing_date,
                d.file_url,
                COALESCE(table_counts.table_count, 0) as table_count,
                COALESCE(type_breakdown.type_1a_count, 0) as type_1a_count,
                COALESCE(type_breakdown.type_1b_count, 0) as type_1b_count,
                COALESCE(type_breakdown.type_2_count, 0) as type_2_count,
                COALESCE(type_breakdown.unknown_count, 0) as unknown_count
            FROM sec_app.documents d
            LEFT JOIN (
                SELECT document_id, COUNT(*) as table_count
                FROM sec_app.document_table_analysis
                GROUP BY document_id
            ) table_counts ON d.id = table_counts.document_id
            LEFT JOIN (
                SELECT
                    document_id,
                    SUM(CASE WHEN table_type = 'type_1a' THEN 1 ELSE 0 END) as type_1a_count,
                    SUM(CASE WHEN table_type = 'type_1b' THEN 1 ELSE 0 END) as type_1b_count,
                    SUM(CASE WHEN table_type = 'type_2' THEN 1 ELSE 0 END) as type_2_count,
                    SUM(CASE WHEN table_type = 'unknown' THEN 1 ELSE 0 END) as unknown_count
                FROM sec_app.document_table_analysis
                GROUP BY document_id
            ) type_breakdown ON d.id = type_breakdown.document_id
            WHERE d.company_id = :company_id AND d.form_type = '10-K'
            ORDER BY d.filing_date DESC
        """), {'company_id': unh.id}).fetchall()

        return [{
            'id': row.id,
            'accession_number': row.accession_number,
            'filing_date': row.filing_date.isoformat() if row.filing_date else None,
            'year': row.filing_date.year if row.filing_date else None,
            'file_url': row.file_url,
            'table_count': row.table_count,
            'type_breakdown': {
                'type_1a': row.type_1a_count,
                'type_1b': row.type_1b_count,
                'type_2': row.type_2_count,
                'unknown': row.unknown_count
            }
        } for row in results]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get UNH documents: {str(e)}")
