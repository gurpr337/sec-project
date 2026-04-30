import requests
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from .extractors.table_extraction_manager import TableExtractionManager
from ..models import FinancialTable, FinancialTableGroup, ColumnHeader, FinancialMetric, DataPoint, Document
from .table_grouping_service import TableGroupingService
from .metric_mapping_service import MetricMappingService

load_dotenv()

class IngestionService:
    def __init__(self, table_grouping_service: TableGroupingService, metric_mapping_service: MetricMappingService):
        self.extraction_manager = TableExtractionManager()
        self.table_grouping_service = table_grouping_service
        self.metric_mapping_service = metric_mapping_service
        self.user_agent = "SECExtractor/1.0 (contact@example.com)"

    def _fetch_html(self, filing_url: str) -> str:
        """Fetch HTML content from SEC filing URL"""
        headers = {
            "User-Agent": self.user_agent,
            "From": "contact@example.com",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Referer": "https://www.sec.gov/edgar/search/",
            "Cache-Control": "no-cache",
        }

        try:
            response = requests.get(filing_url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Error fetching HTML from {filing_url}: {e}")
            return None

    def _extract_table_title_simple(self, table, soup) -> str:
        """Extract table title from surrounding context"""
        candidates = []

        # Method 1: Check for caption
        caption = table.find('caption')
        if caption:
            text = caption.get_text(strip=True)
            if text:
                candidates.append(text)

        # Method 2: Check preceding elements
        for sibling in table.find_previous_siblings(limit=5):
            text = sibling.get_text().strip()
            if len(text) > 10 and len(text) < 200:
                candidates.append(text)

        # Method 3: Check parent elements
        parent = table.parent
        if parent and parent.name in ['div', 'section']:
            heading = parent.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            if heading:
                text = heading.get_text(strip=True)
                if text:
                    candidates.append(text)

        # Return best candidate
        for candidate in candidates:
            if len(candidate) > 5:
                return candidate.strip()

        return "Untitled Table"

    def ingest_filing(self, db: Session, document: Document):
        """
        Ingest a single filing:
        1. Extract structured table data from the filing URL.
        2. Create all new database objects (FinancialTable, ColumnHeader, etc.).
        3. Assign each table to a TableGroup.
        4. Map each metric to a CanonicalMetric.
        """
        # Fetch HTML content
        html_content = self._fetch_html(document.file_url)
        if html_content is None:
            print(f"Failed to fetch HTML for {document.file_url}")
            return

        # Check if we need to re-fetch for comprehensive income tables
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        has_comprehensive_income = any(
            'comprehensive income' in self._extract_table_title_simple(table, soup).lower()
            for table in soup.find_all('table')
        )

        if has_comprehensive_income:
            print("DEBUG: Found comprehensive income table, re-fetching HTML directly from SEC")
            html_content = self._fetch_html(document.file_url)  # Re-fetch to ensure we have the right HTML
            if html_content:
                soup = BeautifulSoup(html_content, 'html.parser')

        # Extract tables using the new extraction manager
        parsed_tables = self.extraction_manager.extract_all_tables(html_content, str(document.id))

        for table_data in parsed_tables:
            # Check if table already exists (for testing - avoid duplicates)
            existing_table = db.query(FinancialTable).filter(
                FinancialTable.document_id == document.id,
                FinancialTable.title == table_data['title']
            ).first()

            if existing_table:
                print(f"DEBUG: Table already exists, updating: {table_data['title']}")
                financial_table = existing_table
                # Delete existing data in correct order (respecting foreign keys)
                # First delete data points
                metric_ids = db.query(FinancialMetric.id).filter(FinancialMetric.table_id == financial_table.id).subquery()
                db.query(DataPoint).filter(DataPoint.metric_id.in_(db.query(metric_ids.c.id))).delete()
                # Then delete metrics
                db.query(FinancialMetric).filter(FinancialMetric.table_id == financial_table.id).delete()
                # Then delete headers
                db.query(ColumnHeader).filter(ColumnHeader.table_id == financial_table.id).delete()
                db.commit()
            else:
                # Create FinancialTable
                financial_table = FinancialTable(
                    document_id=document.id,
                    title=table_data['title']
                )
                db.add(financial_table)
                db.commit()
                db.refresh(financial_table)

            # Process extracted data rows (old simple format)
            extracted_data = table_data.get('extracted_data', [])
            headers = table_data.get('headers', [])

            # Create ColumnHeaders from the simple headers list
            table_type = table_data.get('table_type', 'unknown')
            print(f"DEBUG ingestion: Creating column headers for table {table_data.get('table_index', 'unknown')} (type: {table_type}), headers: {headers}")
            header_map = {}
            for i, header_name in enumerate(headers):
                # Skip metric column headers (usually the first one in standard tables)
                if header_name.lower().strip() in ['metric', 'item', 'description', 'account']:
                    continue

                print(f"DEBUG ingestion: Creating column header '{header_name}'")

                # Check if this header name already exists for this table
                existing_header = db.query(ColumnHeader).filter(
                    ColumnHeader.table_id == financial_table.id,
                    ColumnHeader.flattened_name == header_name
                ).first()

                if existing_header:
                    # Use existing header
                    header_map[header_name] = existing_header.id
                    print(f"DEBUG ingestion: Using existing header {existing_header.id}")
                else:
                    # Create new header
                    column_header = ColumnHeader(
                        table_id=financial_table.id,
                        raw_name=header_name,
                        flattened_name=header_name,  # Use the name as-is for flattened
                        level=0  # Simple headers are all at level 0
                    )
                    db.add(column_header)
                    db.commit()
                    db.refresh(column_header)
                    header_map[header_name] = column_header.id
                    print(f"DEBUG ingestion: Created new header {column_header.id}")

            print(f"DEBUG ingestion: Created {len(header_map)} column headers")

            # Process extracted data rows
            current_section = "default"
            for row_idx, row in enumerate(extracted_data):
                if not row:
                    continue

                # Check if this is a section header row
                if row[0].get('is_section_header', False):
                    # This is a section header - update current section context
                    section_name = row[0].get('text', '').strip()
                    if section_name:
                        current_section = section_name
                        canonical_metric = self.metric_mapping_service.get_or_create_canonical_metric(db, section_name)
                        financial_metric = FinancialMetric(
                            table_id=financial_table.id,
                            raw_name=section_name,
                            flattened_name=section_name,
                            is_section_header=True,
                            canonical_metric_id=canonical_metric.id,
                            cell_coordinates=row[0].get('coordinates', {'row': row_idx, 'col': 0})
                        )
                        db.add(financial_metric)
                        db.commit()
                        db.refresh(financial_metric)
                    continue

                # Regular data row
                if len(row) < 2:
                    continue

                # First cell is the metric name
                metric_cell = row[0]
                metric_name = metric_cell.get('text', '').strip()

                if not metric_name:
                    continue

                # Check if this is unit information in parentheses
                if (metric_name.startswith('(') and metric_name.endswith(')') and
                    ('millions' in metric_name.lower() or 'thousands' in metric_name.lower() or
                     'billions' in metric_name.lower() or 'in millions' in metric_name.lower() or
                     'in thousands' in metric_name.lower() or 'in billions' in metric_name.lower())):
                    # Extract unit from the text
                    if 'millions' in metric_name.lower():
                        unit = 'millions'
                    elif 'billions' in metric_name.lower():
                        unit = 'billions'
                    else:
                        unit = 'thousands'
                    # Update table unit if not already set
                    if not financial_table.unit:
                        financial_table.unit = unit
                        db.commit()
                    continue  # Skip creating a metric for this row

                # Get section context from the row data
                row_section_context = row[0].get('section_context', 'default')

                # Create flattened metric name using section context
                if row_section_context and row_section_context != "default":
                    # Clean section name (remove colons, extra spaces)
                    clean_section = row_section_context.rstrip(':').strip()
                    flattened_name = f"{clean_section} :: {metric_name}"
                else:
                    flattened_name = metric_name

                # Create FinancialMetric
                canonical_metric = self.metric_mapping_service.get_or_create_canonical_metric(db, flattened_name)

                financial_metric = FinancialMetric(
                    table_id=financial_table.id,
                    raw_name=metric_name,
                    flattened_name=flattened_name,
                    is_section_header=False,
                    canonical_metric_id=canonical_metric.id,
                    cell_coordinates=metric_cell.get('coordinates', {'row': row_idx, 'col': 0})
                )
                db.add(financial_metric)
                db.commit()
                db.refresh(financial_metric)

                # Create DataPoints by mapping numeric data cells to headers
                # SEC tables have irregular layouts, so collect numeric cells and map them
                # more intelligently to handle different table formats
                data_cells = []
                for cell_idx, cell in enumerate(row[1:], 1):  # Skip metric name cell
                    cell_text = cell.get('text', '').strip()
                    # Include cells that have digits, are numeric placeholders, or could be zero
                    if cell_text and (any(c.isdigit() for c in cell_text) or cell_text in ['-', '—', '0', '0.0', '0.00']):
                        data_cells.append((cell_idx, cell))

                # Map data cells to headers - for Type 2 tables, skip the 'Metric' header
                if table_type == 'type_2':
                    data_headers = headers[1:]  # Skip 'Metric' header
                else:
                    data_headers = headers

                # If we have data cells, map them to available headers
                if data_cells:
                    # For tables where data cells align with headers, map sequentially
                    # For tables with irregular layouts, distribute data cells across available headers
                    headers_to_map = data_headers[:len(data_cells)] if len(data_headers) > len(data_cells) else data_headers

                    for i, (cell_idx, cell) in enumerate(data_cells):
                        if i < len(headers_to_map):
                            header_name = headers_to_map[i]
                            cell_text = cell.get('text', '').strip()

                            # Parse numeric value
                            if cell_text:
                                try:
                                    cleaned_value = cell_text.replace(',', '').replace('$', '').replace('(', '-').replace(')', '').strip()
                                    if cleaned_value in ['—', '-', 'N/A', '', '0', '0.0', '0.00']:
                                        numeric_value = None
                                    else:
                                        numeric_value = float(cleaned_value)
                                except (ValueError, AttributeError):
                                    numeric_value = None
                            else:
                                numeric_value = None

                            # Get header_id from our map
                            header_id = header_map.get(header_name)

                            if header_id:
                                data_point = DataPoint(
                                    metric_id=financial_metric.id,
                                    header_id=header_id,
                                    value=numeric_value,
                                    cell_coordinates=cell.get('coordinates', {'row': row_idx, 'col': cell_idx})
                                )
                                db.add(data_point)
            
            # Simple table grouping without complex sections
            self.table_grouping_service.get_or_create_table_group(
                db, financial_table, [{'flattened_name': h, 'raw_name': h, 'level': 0} for h in headers[1:]],  # Skip first header
                sections=[],
                num_rows=len(extracted_data),
                num_cols=len(headers) - 1  # Exclude metric column
            )

            db.commit()
