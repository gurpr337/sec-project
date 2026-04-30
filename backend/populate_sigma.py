#!/usr/bin/env python3
"""
Script to populate the sigma table from existing data points.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.sigma_service import SigmaService
from app.database import SessionLocal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting sigma table population...")

    sigma_service = SigmaService()
    db = SessionLocal()

    try:
        # Populate sigma table
        count = sigma_service.populate_sigma_table(db)
        logger.info(f"Successfully populated sigma table with {count} records")

        # Show sample records
        records = sigma_service.get_sigma_records(limit=5)
        logger.info("Sample sigma records:")
        for record in records:
            logger.info(f"  Metric: {record.metric[:50]}..., Period: {record.period_ended}, Value: {record.value}")

        # Export to CSV with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"exports/sigma_data_export_{timestamp}.csv"
        csv_content = sigma_service.export_sigma_to_csv(output_filename, db)
        record_count = len(csv_content.split('\n')) - 1
        logger.info(f"Exported {record_count} records to {output_filename}")

        # Also show the full path
        import os
        full_path = os.path.abspath(output_filename)
        logger.info(f"CSV file saved to: {full_path}")

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":
    main()
