import sys
import os

# Dynamic path to backend
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Company, Document
from app.services.ingestion_service import IngestionService
from app.services.table_grouping_service import TableGroupingService
from app.services.metric_mapping_service import MetricMappingService
from app.services.embedding_service import EmbeddingService
from app.services.pinecone_service import PineconeService
from datetime import datetime

def test_ingest_unh():
    db: Session = SessionLocal()

    try:
        # Check if UNH company exists
        company = db.query(Company).filter(Company.ticker == 'UNH').first()
        if not company:
            print("UNH company not found, creating...")
            company = Company(ticker='UNH', name='UnitedHealth Group Incorporated')
            db.add(company)
            db.commit()
            db.refresh(company)

        # Check if document exists
        accession_number = '0000731766-25-000236'
        document = db.query(Document).filter(Document.accession_number == accession_number).first()
        if not document:
            print("Document not found, creating...")
            document = Document(
                company_id=company.id,
                accession_number=accession_number,
                form_type='10-Q',
                filing_date=datetime(2025, 6, 30),
                file_url='https://www.sec.gov/Archives/edgar/data/731766/000073176625000236/unh-20250630.htm'
            )
            db.add(document)
            db.commit()
            db.refresh(document)

        # Initialize services
        embedding_service = EmbeddingService()
        pinecone_service = PineconeService()
        metric_mapping_service = MetricMappingService(embedding_service, pinecone_service)
        table_grouping_service = TableGroupingService(embedding_service, pinecone_service)
        ingestion_service = IngestionService(table_grouping_service, metric_mapping_service)

        # Ingest the filing
        print("Starting ingestion...")
        ingestion_service.ingest_filing(db, document)
        print("Ingestion completed")

        # Check if the table exists
        from app.models import FinancialTable
        tables = db.query(FinancialTable).filter(FinancialTable.document_id == document.id).all()
        print(f"Found {len(tables)} tables for this document")

        target_tables = []
        for i, table in enumerate(tables):
            title = table.title.lower()
            if 'loss on sale of subsidiary' in title or 'subsidiaries held for sale' in title:
                target_tables.append((i, table))

        print(f"Found {len(target_tables)} tables with target title")

        for i, (table_idx, table) in enumerate(target_tables):
            print(f"\n--- Table {i+1} at DB index {table_idx}: {table.title} ---")

            # Check metrics
            from app.models import FinancialMetric
            metrics = db.query(FinancialMetric).filter(FinancialMetric.table_id == table.id).all()
            print(f"Table has {len(metrics)} metrics")

            if metrics:
                # Check if metrics have "::" delimiter (transformed)
                has_double_colon = any('::' in metric.flattened_name for metric in metrics)
                print(f"Metrics have '::' delimiter: {has_double_colon}")

                for metric in metrics[:5]:  # First 5
                    print(f"  Metric: {metric.raw_name} -> {metric.flattened_name}")

            # Check column headers
            from app.models import ColumnHeader
            headers = db.query(ColumnHeader).filter(ColumnHeader.table_id == table.id).all()
            print(f"Table has {len(headers)} column headers")
            for header in headers[:3]:  # First 3
                print(f"  Header: {header.raw_name} -> {header.flattened_name}")

            # Check data points
            if metrics:
                from app.models import DataPoint
                metric_ids = [m.id for m in metrics]
                data_points = db.query(DataPoint).filter(DataPoint.metric_id.in_(metric_ids)).all()
                print(f"Table has {len(data_points)} data points")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    test_ingest_unh()