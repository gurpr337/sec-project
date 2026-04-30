from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
import json

from ..database import get_db
from .. import schemas
from ..models import Company, Document, FinancialTable, FinancialTableGroup
from ..schemas.company import CompanyCreate, CompanyUpdate, CompanyResponse

router = APIRouter()

@router.get("/", response_model=List[CompanyResponse])
async def get_companies(db: Session = Depends(get_db)):
    companies = db.query(Company).all()
    return companies

@router.get("/{company_ticker}/financial-table-groups", response_model=List[schemas.FinancialTableGroup])
async def get_company_financial_table_groups(company_ticker: str, db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.ticker == company_ticker.upper()).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    # Find table groups that have tables from this company's documents
    table_groups = db.query(FinancialTableGroup).filter(
        FinancialTableGroup.id.in_(
            db.query(FinancialTable.table_group_id).join(Document).filter(Document.company_id == company.id)
        )
    ).all()

    return table_groups

@router.get("/{company_id}", response_model=CompanyResponse)
async def get_company(company_id: int, db: Session = Depends(get_db)):
    company = db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return company

@router.post("/", response_model=CompanyResponse, status_code=status.HTTP_201_CREATED)
async def create_company(company: CompanyCreate, db: Session = Depends(get_db)):
    existing_company = db.query(Company).filter(Company.ticker == company.ticker.upper()).first()
    if existing_company:
        raise HTTPException(status_code=400, detail="Company with this ticker already exists")
    
    db_company = Company(
        ticker=company.ticker.upper(),
        name=company.name
    )
    
    db.add(db_company)
    db.commit()
    db.refresh(db_company)
    
    return db_company
