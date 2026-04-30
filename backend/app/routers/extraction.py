from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
import os
import time
from sqlalchemy.orm import Session
from typing import List, Dict, Any
import json
from datetime import datetime
import traceback

from .. import models, schemas
from ..database import get_db, SessionLocal
from ..models import Company, Document, FinancialTable, FinancialMetric, DataPoint, ColumnHeader, CanonicalMetric, FinancialTableGroup
from ..services.sec_extractor import SECExtractor
from ..services.embedding_service import EmbeddingService
from ..services.pinecone_service import PineconeService
from ..services.metric_mapping_service import MetricMappingService
from ..services.table_grouping_service import TableGroupingService
from ..services.ingestion_service import IngestionService

router = APIRouter()

# Initialize services
embedding_service = EmbeddingService()
pinecone_service = PineconeService()
metric_mapping_service = MetricMappingService(embedding_service, pinecone_service)
table_grouping_service = TableGroupingService(embedding_service, pinecone_service)
ingestion_service = IngestionService(table_grouping_service, metric_mapping_service)

@router.post("/start", status_code=status.HTTP_202_ACCEPTED)
async def start_extraction_job(
    company_ticker: str,
    form_types: str,  # Expecting comma-separated string e.g., "10-K,10-Q"
    start_date: str,
    end_date: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    company = db.query(Company).filter(Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail=f"Company {company_ticker} not found")

    form_types_list = [ft.strip().upper() for ft in form_types.split(',')]

    job = models.ExtractionJob(
        company_id=company.id,
        status="pending",
        job_type="initial",
        job_metadata={
            "company_ticker": company.ticker,
            "form_types": form_types_list,
            "start_date": start_date,
            "end_date": end_date,
        }
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    background_tasks.add_task(run_extraction_job, job.id, company.id, form_types_list, start_date, end_date)

    return {"message": "Extraction started", "id": job.id}


@router.get("/jobs/{job_id}", response_model=schemas.ExtractionJob)
def get_job_status(job_id: int, db: Session = Depends(get_db)):
    job = db.query(models.ExtractionJob).filter(models.ExtractionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

def run_extraction_job(job_id: int, company_id: int, form_types: List[str], start_date: str, end_date: str):
    """Run the extraction job"""
    print(f"Starting extraction job {job_id}")
    db: Session = SessionLocal()

    try:
        job = db.query(models.ExtractionJob).filter(models.ExtractionJob.id == job_id).first()
        if not job:
            print(f"ERROR: Job {job_id} not found.")
            return

        job.status = "running"
        db.commit()
        print(f"Job {job_id} status set to running")

        company = db.query(Company).filter(Company.id == company_id).first()
        if not company:
            job.status = "failed"
            job.error_message = f"Company {company_id} not found."
            db.commit()
            return

        print(f"Processing company {company.ticker}")
        filings = SECExtractor().get_filings(company.ticker, form_types, start_date, end_date)
        print(f"Found {len(filings)} filings")

        processed_count = 0
        for filing in filings:
            accession_number = filing.get('accessionNo')
            if not accession_number:
                continue

            existing_document = db.query(Document).filter(Document.accession_number == accession_number).first()
            if existing_document:
                print(f"Document {accession_number} already exists, skipping")
                continue

            filing_date_str = filing.get('filedAt', '')
            if not filing_date_str:
                continue

            filing_date = datetime.fromisoformat(filing_date_str.replace('Z', '+00:00')) if 'T' in filing_date_str else datetime.strptime(filing_date_str, '%Y-%m-%d')

            document = Document(
                company_id=company.id,
                accession_number=accession_number,
                form_type=filing.get('formType', ''),
                filing_date=filing_date,
                file_url=filing.get('linkToFilingDetails', '')
            )
            db.add(document)
            db.commit()
            db.refresh(document)
            print(f"Created document {accession_number}")

            try:
                ingestion_service.ingest_filing(db, document)
                processed_count += 1
                print(f"Processed document {accession_number} ({processed_count} total)")
            except Exception as e:
                print(f"Error processing document {accession_number}: {e}")
                continue

        job.status = "completed"
        db.commit()
        print(f"Job {job_id} completed successfully, processed {processed_count} documents")

    except Exception as e:
        print(f"Job {job_id} failed with error: {e}")
        db.rollback()
        job = db.query(models.ExtractionJob).filter(models.ExtractionJob.id == job_id).first()
        if job:
            job.status = "failed"
            job.error_message = str(e)
            db.commit()
        traceback.print_exc()
    finally:
        db.close()
