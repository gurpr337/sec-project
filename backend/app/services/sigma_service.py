from sqlalchemy.orm import Session, joinedload
from sqlalchemy import text
from typing import List, Optional
import logging
import re

from ..models import DataPoint, FinancialMetric, ColumnHeader, FinancialTable, Document, Sigma
from ..database import SessionLocal

logger = logging.getLogger(__name__)

class SigmaService:
    def __init__(self):
        pass

    def _is_valid_period(self, period: str) -> bool:
        """
        Check if a period contains both a month and a year using regex.
        Valid periods must contain:
        - Any month name (January, February, etc.)
        - Any 4-digit year (19xx, 20xx, 21xx, 22xx, etc.)
        """
        if not period:
            return False

        # Regex to match any month name (case insensitive)
        month_pattern = r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b'

        # Regex to match any 4-digit year
        year_pattern = r'\b\d{4}\b'

        has_month = bool(re.search(month_pattern, period, re.IGNORECASE))
        has_year = bool(re.search(year_pattern, period))

        return has_month and has_year

    def populate_sigma_table(self, db: Optional[Session] = None) -> int:
        """
        Populate the sigma table from existing data points.
        Returns the number of records inserted.
        """
        if db is None:
            db = SessionLocal()

        try:
            # Clear existing sigma records
            db.execute(text("DELETE FROM sec_app.sigma"))
            db.commit()
            logger.info("Cleared existing sigma records")

            # Query all data points with related data
            data_points = (
                db.query(DataPoint)
                .options(
                    joinedload(DataPoint.metric)
                    .joinedload(FinancialMetric.table)
                    .joinedload(FinancialTable.document),
                    joinedload(DataPoint.header)
                )
                .all()
            )

            inserted_count = 0

            for data_point in data_points:
                try:
                    # Skip data points without values
                    if data_point.value is None:
                        continue

                    # Get related objects
                    metric = data_point.metric
                    header = data_point.header
                    table = metric.table
                    document = table.document

                    # Construct period_ended from header flattened_name
                    # Headers like "June 30, 2024" are already in the right format
                    period_ended = header.flattened_name.strip()

                    # Construct source_cell URL
                    # Format: /document-viewer?url={document_url}&row={row}&col={col}&text={value}
                    if data_point.cell_coordinates and document.file_url:
                        row = data_point.cell_coordinates.get('row', 0)
                        col = data_point.cell_coordinates.get('col', 0)
                        source_cell = f"/document-viewer?url={document.file_url}&row={row}&col={col}&text={data_point.value}"
                    else:
                        source_cell = None

                    # Create sigma record
                    sigma_record = Sigma(
                        data_point_id=data_point.id,
                        metric=metric.flattened_name,
                        period_ended=period_ended,
                        value=data_point.value,
                        denomination=table.unit,
                        source_table_name=table.title,
                        source_cell=source_cell
                    )

                    db.add(sigma_record)
                    inserted_count += 1

                    # Commit in batches to avoid memory issues
                    if inserted_count % 1000 == 0:
                        db.commit()
                        logger.info(f"Inserted {inserted_count} sigma records so far...")

                except Exception as e:
                    logger.error(f"Error processing data point {data_point.id}: {e}")
                    continue

            # Final commit
            db.commit()
            logger.info(f"Successfully inserted {inserted_count} records into sigma table")
            return inserted_count

        except Exception as e:
            db.rollback()
            logger.error(f"Error populating sigma table: {e}")
            raise
        finally:
            if db is not SessionLocal():
                db.close()

    def get_sigma_records(self, limit: int = 100, offset: int = 0, db: Optional[Session] = None) -> List[Sigma]:
        """
        Get sigma records with pagination.
        """
        if db is None:
            db = SessionLocal()

        try:
            records = db.query(Sigma).offset(offset).limit(limit).all()
            return records
        finally:
            if db is not SessionLocal():
                db.close()

    def _parse_period_and_date(self, period_text: str) -> tuple[str, str]:
        """Parse a period string into Period and Date components.

        Examples:
        - "Six Months Ended June 30, 2024" → ("Six Months Ended", "June 30, 2024")
        - "June 30, 2024" → ("", "June 30, 2024")
        - "Three Months Ended March 31, 2025" → ("Three Months Ended", "March 31, 2025")
        """
        if not period_text:
            return "", ""

        # Look for date patterns at the end of the string
        # Common patterns: "Month DD, YYYY", "Month DD YYYY", "MM/DD/YYYY", etc.
        import re

        # Pattern for "Month DD, YYYY" at the end
        month_date_pattern = r'\b((?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{1,2},?\s*\d{4})\b$'

        match = re.search(month_date_pattern, period_text.strip())
        if match:
            date_part = match.group(1)
            period_part = period_text[:match.start()].strip()
            # Remove trailing separators like "Ended" if it's just the date
            period_part = period_part.rstrip(' ,-')
            return period_part, date_part

        # If no date pattern found, treat the whole thing as a date
        return "", period_text.strip()

    def export_sigma_to_csv(self, filename: str, db: Optional[Session] = None) -> str:
        """
        Export sigma table to CSV format.
        Only includes records with valid periods (containing month and year).
        Returns the CSV content as a string.
        """
        if db is None:
            db = SessionLocal()

        try:
            records = db.query(Sigma).all()

            # CSV header - now includes Period/Date split and Company
            csv_lines = ["Company,Metric,Period,Date,Value,Denomination,Source Table Name,Source Cell"]

            # CSV data rows - only include valid periods
            valid_records = 0
            invalid_records = 0

            for record in records:
                # Check if period is valid (contains month and year)
                if not self._is_valid_period(record.period_ended):
                    invalid_records += 1
                    # Log first few invalid periods for debugging
                    if invalid_records <= 5:
                        logger.info(f"Invalid period found: '{record.period_ended}'")
                    continue

                # Parse period into Period and Date components
                period_part, date_part = self._parse_period_and_date(record.period_ended)

                # Get company ticker from the relationship: Sigma -> DataPoint -> Table -> Document -> Company
                company_ticker = ""
                try:
                    if record.data_point and record.data_point.metric and record.data_point.metric.table:
                        table = record.data_point.metric.table
                        if table.document and table.document.company:
                            company_ticker = table.document.company.ticker or ""
                except Exception as e:
                    logger.warning(f"Could not get company ticker for record {record.id}: {e}")

                # Escape commas and quotes in fields
                company = str(company_ticker or "").replace('"', '""')
                metric = str(record.metric or "").replace('"', '""')
                period = str(period_part or "").replace('"', '""')
                date = str(date_part or "").replace('"', '""')
                value = str(record.value or "").replace('"', '""')
                denomination = str(record.denomination or "").replace('"', '""')
                table_name = str(record.source_table_name or "").replace('"', '""')
                source_cell = str(record.source_cell or "").replace('"', '""')

                # Quote fields that contain commas
                if ',' in company: company = f'"{company}"'
                if ',' in metric: metric = f'"{metric}"'
                if ',' in period: period = f'"{period}"'
                if ',' in date: date = f'"{date}"'
                if ',' in value: value = f'"{value}"'
                if ',' in denomination: denomination = f'"{denomination}"'
                if ',' in table_name: table_name = f'"{table_name}"'
                if ',' in source_cell: source_cell = f'"{source_cell}"'

                line = f"{company},{metric},{period},{date},{value},{denomination},{table_name},{source_cell}"
                csv_lines.append(line)
                valid_records += 1

            logger.info(f"Period validation: {valid_records} valid records, {invalid_records} invalid records skipped")

            csv_content = "\n".join(csv_lines)

            # Write to file if filename provided
            if filename:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(csv_content)

            return csv_content

        finally:
            if db is not SessionLocal():
                db.close()
