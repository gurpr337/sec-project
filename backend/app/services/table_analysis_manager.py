"""
Table Analysis Manager - analyzes all tables in UNH documents using cloned Type 1B logic
"""
import requests
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from sqlalchemy import text
from .extractors.analysis_base_extractor import AnalysisBaseExtractor
from .extractors.analysis_type1b_extractor import AnalysisType1B_Extractor


class TableAnalysisManager:
    """
    Analyzes all tables in UNH 10-K documents using cloned extraction logic.
    Stores results in document_table_analysis table without affecting production data.
    """

    def __init__(self):
        self.base_extractor = AnalysisBaseExtractor()
        self.type1b_extractor = AnalysisType1B_Extractor()

    def analyze_unh_documents(self, db: Session) -> Dict[str, Any]:
        """
        Analyze all tables in all UNH 10-K documents
        Returns summary of analysis results
        """
        # Get UNH company
        from ..models.company import Company
        unh = db.query(Company).filter(Company.ticker == 'UNH').first()
        if not unh:
            return {"error": "UNH company not found"}

        # Get all UNH 10-K documents
        from ..models.document import Document
        documents = db.query(Document).filter(
            Document.company_id == unh.id,
            Document.form_type == '10-K'
        ).order_by(Document.filing_date.desc()).all()

        total_tables = 0
        type_counts = {'type_1a': 0, 'type_1b': 0, 'type_2': 0, 'unknown': 0}
        processed_documents = []

        for doc in documents:
            year = doc.filing_date.year if doc.filing_date else 'N/A'
            print(f"Analyzing {year} document ({doc.accession_number})...")

            try:
                # Fetch document HTML via proxy
                proxy_url = f'http://localhost:8000/api/proxy?url={doc.file_url}'
                response = requests.get(proxy_url, timeout=30)

                if response.status_code == 200:
                    html_content = response.text
                    soup = BeautifulSoup(html_content, 'html.parser')

                    # Check if this is an index page - if so, find the actual document URL
                    # For older filings (2015 and before), SEC stores index pages with document links in tables
                    is_index_page = ('index.htm' in doc.file_url or
                                   (soup.title and 'edgar' in soup.title.get_text().lower()) or
                                   (year <= 2015))  # Older documents often use index pages

                    if is_index_page:
                        # This is an index page, find the actual 10-K HTML document
                        actual_doc_url = None

                        # First, try to find the document URL from the filing table
                        tables = soup.find_all('table')
                        for table in tables:
                            rows = table.find_all('tr')
                            for row in rows:
                                cells = row.find_all(['td', 'th'])
                                if len(cells) >= 3:  # Need at least sequence, form, document columns
                                    cell_texts = [cell.get_text(strip=True) for cell in cells]
                                    # Look for row where second column is "10-K"
                                    if len(cell_texts) >= 2 and cell_texts[1] == '10-K':
                                        # Third column should have the document filename
                                        if len(cell_texts) >= 3 and cell_texts[2].endswith('.htm'):
                                            doc_filename = cell_texts[2]
                                            # Construct full URL
                                            cik = doc.accession_number.split("-")[0]
                                            accession_no_dashes = doc.accession_number.replace("-", "")
                                            actual_doc_url = f'https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/{doc_filename}'
                                            print(f"  Found 10-K document from table: {doc_filename}")
                                            break
                                if actual_doc_url:
                                    break
                            if actual_doc_url:
                                break

                        # Fallback: look for links if table parsing didn't work
                        if not actual_doc_url:
                            links = soup.find_all('a', href=True)
                            for link in links:
                                href = link['href']
                                if '.htm' in href and '10-k' in href.lower() and 'ex' not in href.lower():
                                    # Construct full URL
                                    if href.startswith('/'):
                                        actual_doc_url = f'https://www.sec.gov{href}'
                                    else:
                                        actual_doc_url = href
                                    break

                        if actual_doc_url:
                            print(f"  Found actual document URL: {actual_doc_url}")
                            # Fetch the actual document
                            proxy_url = f'http://localhost:8000/api/proxy?url={actual_doc_url}'
                            response = requests.get(proxy_url, timeout=30)
                            if response.status_code == 200:
                                html_content = response.text
                                soup = BeautifulSoup(html_content, 'html.parser')
                            else:
                                print(f"  Failed to fetch actual document: {response.status_code}")
                                continue
                        else:
                            print(f"  Could not find actual document URL in index page")
                            continue

                    # Find all tables in the document
                    tables = soup.find_all('table')
                    doc_table_count = 0

                    for table_idx, table in enumerate(tables):
                        # Analyze this table
                        analysis_result = self.analyze_single_table(table, table_idx, doc, soup)

                        if analysis_result:
                            # Store in database
                            self._store_analysis_result(db, analysis_result)
                            doc_table_count += 1
                            type_counts[analysis_result['table_type']] += 1

                    total_tables += doc_table_count
                    processed_documents.append({
                        'year': year,
                        'accession_number': doc.accession_number,
                        'tables_found': doc_table_count
                    })

                    print(f"  Found {doc_table_count} tables in {year} document")
                else:
                    print(f"  ERROR: Failed to fetch {year} document (HTTP {response.status_code})")

            except Exception as e:
                print(f"  ERROR: Failed to analyze {year} document: {str(e)}")

        # Commit all changes
        db.commit()

        return {
            'total_documents': len(processed_documents),
            'total_tables': total_tables,
            'type_breakdown': type_counts,
            'documents_processed': processed_documents
        }

    def analyze_single_table(self, table, table_idx: int, document, soup) -> Dict:
        """
        Analyze a single table using the cloned extraction logic
        Returns analysis result or None if table should be skipped
        """
        # Classify table type using cloned logic
        table_type, classification_reason = self.base_extractor.classify_table_type(table)

        # Count header rows for analysis
        header_rows = self.base_extractor._identify_header_rows_for_simple(table)
        header_rows_count = len(header_rows)

        # Check for hierarchical dates
        has_hierarchical_dates = self.base_extractor._has_hierarchical_date_patterns(header_rows)

        # Process ALL tables as Type 1B for now (user requirement)
        title = self.base_extractor.extract_table_title(table, soup)

        # Force all tables to be treated as Type 1B
        forced_table_type = 'type_1b'
        forced_classification_reason = f'Forced Type 1B processing (original classification: {table_type} - {classification_reason})'

        # Use Type 1B extractor for all tables
        result = self.type1b_extractor.process_table_analysis(table, soup)
        if result:
            result.update({
                'table_type': forced_table_type,
                'classification_reason': forced_classification_reason,
                'header_rows_count': header_rows_count,
                'has_hierarchical_dates': has_hierarchical_dates,
                'document_id': document.id,
                'table_index': table_idx,
                'title': title,
                'original_html': str(table)
            })
            return result

        # Fallback to basic extraction if Type 1B extraction fails or returns empty
        print(f"Type 1B extractor failed for table {table_idx}, using basic extraction")
        result = self._extract_basic_table_data(table)
        if result:
            result.update({
                'table_type': forced_table_type,
                'classification_reason': f'{forced_classification_reason} (used basic extraction fallback)',
                'header_rows_count': header_rows_count,
                'has_hierarchical_dates': has_hierarchical_dates,
                'document_id': document.id,
                'table_index': table_idx,
                'title': title,
                'original_html': str(table)
            })
            return result

        # Final fallback
        return {
            'table_type': forced_table_type,
            'classification_reason': f'Forced Type 1B failed completely (original: {table_type} - {classification_reason})',
            'header_rows_count': header_rows_count,
            'has_hierarchical_dates': has_hierarchical_dates,
            'document_id': document.id,
            'table_index': table_idx,
            'original_html': str(table),
            'title': title,
            'headers': [],
            'extracted_data': [],
            'num_rows': 0,
            'num_cols': 0,
            'content_hash': ''
        }

        return None

    def _extract_basic_table_data(self, table) -> Dict:
        """
        Extract basic tabular data from tables - very simple approach
        Just extract all cell text in a grid structure
        """
        rows = table.find_all('tr')
        if len(rows) < 1:
            return None

        extracted_data = []
        max_cols = 0

        # First pass: find maximum columns
        for row in rows:
            cells = row.find_all(['td', 'th'])
            max_cols = max(max_cols, len(cells))

        # Second pass: extract data
        for row_idx, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])
            row_data = []

            for col_idx in range(max_cols):
                if col_idx < len(cells):
                    cell = cells[col_idx]
                    text = cell.get_text(strip=True)
                else:
                    text = ''

                row_data.append({
                    'text': text,
                    'coordinates': {'row': row_idx, 'col': col_idx},
                    'is_section_header': False,
                    'section_context': 'default'
                })

            extracted_data.append(row_data)

        # Use first row as headers, rest as data
        headers = []
        data_rows = extracted_data

        if extracted_data:
            headers = [cell['text'] for cell in extracted_data[0]]
            data_rows = extracted_data[1:]

        return {
            'headers': headers,
            'extracted_data': data_rows,
            'num_rows': len(data_rows),
            'num_cols': len(headers),
            'content_hash': ''
        }

    def _store_analysis_result(self, db: Session, result: Dict):
        """Store analysis result in the database"""
        import json

        # Insert into document_table_analysis table
        insert_sql = text("""
            INSERT INTO sec_app.document_table_analysis (
                document_id, table_index, original_html, table_type,
                parsed_headers, parsed_data, title, classification_reason,
                header_rows_count, has_hierarchical_dates
            ) VALUES (
                :document_id, :table_index, :original_html, :table_type,
                :parsed_headers, :parsed_data, :title, :classification_reason,
                :header_rows_count, :has_hierarchical_dates
            )
        """)

        db.execute(insert_sql, {
            'document_id': result['document_id'],
            'table_index': result['table_index'],
            'original_html': result['original_html'],
            'table_type': result['table_type'],
            'parsed_headers': json.dumps(result.get('headers', [])),
            'parsed_data': json.dumps(result.get('extracted_data', [])),
            'title': result.get('title', ''),
            'classification_reason': result['classification_reason'],
            'header_rows_count': result['header_rows_count'],
            'has_hierarchical_dates': result['has_hierarchical_dates']
        })

    def get_analysis_summary(self, db: Session) -> Dict[str, Any]:
        """Get summary of all analysis results"""
        # Count by table type
        type_counts = db.execute(text("""
            SELECT table_type, COUNT(*) as count
            FROM sec_app.document_table_analysis
            GROUP BY table_type
        """)).fetchall()

        # Count by document year
        document_counts = db.execute(text("""
            SELECT
                EXTRACT(YEAR FROM d.filing_date) as year,
                COUNT(da.id) as table_count
            FROM sec_app.document_table_analysis da
            JOIN sec_app.documents d ON da.document_id = d.id
            GROUP BY EXTRACT(YEAR FROM d.filing_date)
            ORDER BY year DESC
        """)).fetchall()

        # Total counts
        total_result = db.execute(text("""
            SELECT COUNT(*) as total_tables,
                   COUNT(DISTINCT document_id) as total_documents
            FROM sec_app.document_table_analysis
        """)).first()

        return {
            'total_tables': total_result.total_tables,
            'total_documents': total_result.total_documents,
            'type_breakdown': dict(type_counts),
            'document_breakdown': [{'year': int(row.year), 'tables': row.table_count} for row in document_counts]
        }

    def get_document_tables(self, db: Session, document_id: int) -> List[Dict]:
        """Get all analyzed tables for a specific document"""
        results = db.execute(text("""
            SELECT id, table_index, table_type, title, classification_reason,
                   header_rows_count, has_hierarchical_dates,
                   original_html, parsed_headers, parsed_data
            FROM sec_app.document_table_analysis
            WHERE document_id = :document_id
            ORDER BY table_index
        """), {'document_id': document_id}).fetchall()

        # Ensure JSON fields are properly parsed
        import json
        tables = []
        for row in results:
            try:
                if isinstance(row.parsed_headers, (list, dict)):
                    parsed_headers = row.parsed_headers
                else:
                    parsed_headers = json.loads(row.parsed_headers or '[]')

                if isinstance(row.parsed_data, (list, dict)):
                    parsed_data = row.parsed_data
                else:
                    parsed_data = json.loads(row.parsed_data or '[]')
            except (json.JSONDecodeError, TypeError):
                parsed_headers = []
                parsed_data = []

            tables.append({
                'id': row.id,
                'table_index': row.table_index,
                'table_type': row.table_type,
                'title': row.title,
                'classification_reason': row.classification_reason,
                'header_rows_count': row.header_rows_count,
                'has_hierarchical_dates': row.has_hierarchical_dates,
                'original_html': row.original_html,
                'parsed_headers': parsed_headers,
                'parsed_data': parsed_data
            })

        return tables

    def get_table_by_id(self, db: Session, table_id: int) -> Dict:
        """Get a specific analyzed table by ID"""
        result = db.execute(text("""
            SELECT da.*, d.filing_date, d.accession_number
            FROM sec_app.document_table_analysis da
            JOIN sec_app.documents d ON da.document_id = d.id
            WHERE da.id = :table_id
        """), {'table_id': table_id}).first()

        if result:
            # Ensure JSON fields are properly parsed
            import json
            try:
                if isinstance(result.parsed_headers, (list, dict)):
                    parsed_headers = result.parsed_headers
                else:
                    parsed_headers = json.loads(result.parsed_headers or '[]')

                if isinstance(result.parsed_data, (list, dict)):
                    parsed_data = result.parsed_data
                else:
                    parsed_data = json.loads(result.parsed_data or '[]')
            except (json.JSONDecodeError, TypeError):
                parsed_headers = []
                parsed_data = []

            return {
                'id': result.id,
                'document_id': result.document_id,
                'table_index': result.table_index,
                'table_type': result.table_type,
                'title': result.title,
                'classification_reason': result.classification_reason,
                'header_rows_count': result.header_rows_count,
                'has_hierarchical_dates': result.has_hierarchical_dates,
                'original_html': result.original_html,
                'parsed_headers': parsed_headers,
                'parsed_data': parsed_data,
                'filing_date': result.filing_date.isoformat() if result.filing_date else None,
                'accession_number': result.accession_number
            }

        return None
