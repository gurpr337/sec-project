#!/usr/bin/env python3
"""
Script to fetch all UNH 10-K documents from the past 20 years and store them in the database.
"""

import sys
import os
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.database import get_db
from app.models.company import Company
from app.models.document import Document
from app.services.sec_extractor import SECExtractor

def get_or_create_unh_company(db: Session) -> Company:
    """Get or create UnitedHealth Group company in the database."""
    ticker = "UNH"
    name = "UnitedHealth Group Incorporated"

    # Check if company already exists
    company = db.query(Company).filter(Company.ticker == ticker).first()
    if company:
        print(f"Found existing company: {company.name} ({company.ticker})")
        return company

    # Create new company
    company = Company(ticker=ticker, name=name)
    db.add(company)
    db.commit()
    db.refresh(company)
    print(f"Created new company: {company.name} ({company.ticker})")
    return company

def fetch_and_store_unh_10k_documents():
    """Fetch all UNH 10-K documents from the past 20 years and store in database."""
    print("Starting UNH 10-K document fetch...")

    # Calculate date range (past 20 years)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*20)  # 20 years ago

    print(f"Fetching 10-K filings from {start_date.date()} to {end_date.date()}")

    # Initialize SEC extractor
    sec_extractor = SECExtractor()

    # Get database session
    db = next(get_db())

    try:
        # Get or create UNH company
        company = get_or_create_unh_company(db)

        # Fetch 10-K filings
        print("Fetching filings from SEC API...")
        filings = sec_extractor.get_filings(
            ticker="UNH",
            form_types=["10-K"],
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )

        print(f"Found {len(filings)} 10-K filings")

        # Store each filing in database
        stored_count = 0
        skipped_count = 0

        for filing in filings:
            accession_number = filing.get('accessionNo')
            if not accession_number:
                continue

            # Check if document already exists
            existing_doc = db.query(Document).filter(
                Document.accession_number == accession_number
            ).first()

            if existing_doc:
                print(f"Skipping existing document: {accession_number}")
                skipped_count += 1
                continue

            # Parse filing date
            filed_at = filing.get('filedAt')
            if filed_at:
                try:
                    # SEC API returns date in format like "2024-03-01T00:00:00.000Z"
                    filing_date = datetime.fromisoformat(filed_at.replace('Z', '+00:00'))
                except:
                    filing_date = None
            else:
                filing_date = None

            # Create document record
            document = Document(
                company_id=company.id,
                accession_number=accession_number,
                form_type=filing.get('formType', '10-K'),
                filing_date=filing_date,
                file_url=filing.get('linkToHtml'),
                is_processed=False,
                processing_status='pending'
            )

            db.add(document)
            stored_count += 1

            if stored_count % 10 == 0:
                print(f"Processed {stored_count + skipped_count}/{len(filings)} filings...")

        # Commit all changes
        db.commit()
        print("\nSuccessfully stored documents:")
        print(f"  New documents: {stored_count}")
        print(f"  Skipped (already exist): {skipped_count}")
        print(f"  Total documents for UNH: {db.query(Document).filter(Document.company_id == company.id, Document.form_type == '10-K').count()}")

    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    fetch_and_store_unh_10k_documents()
