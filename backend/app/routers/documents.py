from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import extract, desc
from typing import List, Optional
from ..database import get_db
from ..models.document import Document
from ..models.company import Company

router = APIRouter()

@router.get("/", response_model=List[dict])
async def get_documents(
    company_id: int = None,
    form_type: str = None,
    db: Session = Depends(get_db)
):
    """Get all documents with optional filtering"""
    query = db.query(Document)
    
    if company_id:
        query = query.filter(Document.company_id == company_id)
    
    if form_type:
        query = query.filter(Document.form_type == form_type)
    
    documents = query.order_by(Document.filing_date.desc()).all()
    
    return [
        {
            "id": doc.id,
            "company_id": doc.company_id,
            "accession_number": doc.accession_number,
            "form_type": doc.form_type,
            "filing_date": doc.filing_date.isoformat() if doc.filing_date else None,
            "period_ending": doc.period_ending.isoformat() if doc.period_ending else None,
            "file_url": doc.file_url,
            "file_size": doc.file_size,
            "is_processed": doc.is_processed,
            "processing_status": doc.processing_status,
            "error_message": doc.error_message,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
            "updated_at": doc.updated_at.isoformat() if doc.updated_at else None
        }
        for doc in documents
    ]

@router.get("/{document_id}")
async def get_document(document_id: int, db: Session = Depends(get_db)):
    """Get a specific document by ID"""
    document = db.query(Document).filter(Document.id == document_id).first()
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return {
        "id": document.id,
        "company_id": document.company_id,
        "accession_number": document.accession_number,
        "form_type": document.form_type,
        "filing_date": document.filing_date.isoformat() if document.filing_date else None,
        "period_ending": document.period_ending.isoformat() if document.period_ending else None,
        "file_url": document.file_url,
        "file_size": document.file_size,
        "is_processed": document.is_processed,
        "processing_status": document.processing_status,
        "error_message": document.error_message,
        "created_at": document.created_at.isoformat() if document.created_at else None,
        "updated_at": document.updated_at.isoformat() if document.updated_at else None
    }

@router.get("/unh/10k", response_model=List[dict])
async def get_unh_10k_documents(
    sort_by_year: Optional[str] = "desc",
    db: Session = Depends(get_db)
):
    """Get all UNH 10-K documents sorted by filing year"""
    # Find UNH company
    company = db.query(Company).filter(Company.ticker == "UNH").first()
    if not company:
        raise HTTPException(status_code=404, detail="UNH company not found")

    # Query for 10-K documents
    query = db.query(Document).filter(
        Document.company_id == company.id,
        Document.form_type == "10-K"
    )

    # Sort by year (extract year from filing_date)
    if sort_by_year == "desc":
        query = query.order_by(desc(extract('year', Document.filing_date)))
    else:
        query = query.order_by(extract('year', Document.filing_date))

    documents = query.all()

    return [
        {
            "id": doc.id,
            "company_id": doc.company_id,
            "accession_number": doc.accession_number,
            "form_type": doc.form_type,
            "filing_date": doc.filing_date.isoformat() if doc.filing_date else None,
            "year": doc.filing_date.year if doc.filing_date else None,
            "period_ending": doc.period_ending.isoformat() if doc.period_ending else None,
            "file_url": doc.file_url,
            "file_size": doc.file_size,
            "is_processed": doc.is_processed,
            "processing_status": doc.processing_status,
            "error_message": doc.error_message,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
            "updated_at": doc.updated_at.isoformat() if doc.updated_at else None
        }
        for doc in documents
    ]
