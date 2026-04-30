from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from .. import models, schemas
from ..database import get_db
from typing import List

router = APIRouter()

@router.get("/jobs/{job_id}", response_model=schemas.ExtractionJob)
def get_job_status(job_id: int, db: Session = Depends(get_db)):
    job = db.query(models.ExtractionJob).filter(models.ExtractionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@router.get("/{job_id}/documents", response_model=List[schemas.Document])
def get_job_documents(job_id: int, db: Session = Depends(get_db)):
    job = db.query(models.ExtractionJob).filter(models.ExtractionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # This is an approximation: find documents created during the job's runtime
    documents = db.query(models.Document).filter(
        models.Document.company_id == job.company_id,
        models.Document.created_at >= job.created_at
    ).all()
    return documents
