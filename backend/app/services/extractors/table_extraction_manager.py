from typing import List, Dict
from .type1a_extractor import Type1A_Extractor
from .type1b_extractor import Type1B_Extractor
from .type2_extractor import Type2_Extractor


class TableExtractionManager:
    """Coordinates all table extractors for complete isolation"""

    def __init__(self):
        self.type1a_extractor = Type1A_Extractor()
        self.type1b_extractor = Type1B_Extractor()
        self.type2_extractor = Type2_Extractor()

    def extract_all_tables(self, html_content: str, filing_id: str) -> List[Dict]:
        """Extract all tables using appropriate type-specific extractors with proper routing"""

        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html_content, 'html.parser')
        all_tables = []

        # Process each table individually with type detection and routing
        for idx, table in enumerate(soup.find_all('table')):
            table_type = self.type1a_extractor._determine_table_type(table)

            try:
                if table_type == 'type_1a':
                    # Type 1A: Process with Type1A_Extractor
                    result = self._extract_single_table_type1a(table, idx, filing_id, soup)
                    if result:
                        all_tables.append(result)

                elif table_type == 'type_1b':
                    # Type 1B: Process with Type1B_Extractor
                    result = self._extract_single_table_type1b(table, idx, filing_id, soup)
                    if result:
                        all_tables.append(result)

                elif table_type == 'type_2':
                    # Type 2: Process with Type2_Extractor
                    result = self._extract_single_table_type2(table, idx, filing_id, soup)
                    if result:
                        all_tables.append(result)
                # Skip 'unknown' tables silently

            except Exception as e:
                print(f"Error processing table {idx}: {e}")
                continue

        # Sort by table index to maintain original order
        all_tables.sort(key=lambda x: x['table_index'])

        type_counts = {}
        for table in all_tables:
            table_type = table.get('table_type', 'unknown')
            type_counts[table_type] = type_counts.get(table_type, 0) + 1

        print(f"Extraction complete: {type_counts.get('type_1a', 0)} Type 1A, {type_counts.get('type_1b', 0)} Type 1B, {type_counts.get('type_2', 0)} Type 2 tables")

        return all_tables

    def _extract_single_table_type1a(self, table, idx: int, filing_id: str, soup) -> Dict:
        """Extract a single Type 1A table"""
        rows = table.find_all('tr')
        if len(rows) < 2:
            return None

        headers = self.type1a_extractor._extract_type1a_headers(table)
        extracted_data = self.type1a_extractor._extract_type1a_data(table, headers)
        title = self.type1a_extractor.extract_table_title(table, soup)

        return {
            'table_index': idx,
            'filing_id': filing_id,
            'title': title,
            'table_type': 'type_1a',
            'headers': headers,
            'extracted_data': extracted_data,
            'num_rows': len(extracted_data),
            'num_cols': len(headers) + 1,
            'content_hash': self.type1a_extractor._generate_hash(headers, extracted_data),
            'raw_html': str(table)
        }

    def _extract_single_table_type1b(self, table, idx: int, filing_id: str, soup) -> Dict:
        """Extract a single Type 1B table"""
        rows = table.find_all('tr')
        if len(rows) < 2:
            return None

        headers = self.type1b_extractor._extract_type1b_headers(table)
        extracted_data = self.type1b_extractor._extract_type1b_data(table, headers)
        title = self.type1b_extractor.extract_table_title(table, soup)

        return {
            'table_index': idx,
            'filing_id': filing_id,
            'title': title,
            'table_type': 'type_1b',
            'headers': headers,
            'extracted_data': extracted_data,
            'num_rows': len(extracted_data),
            'num_cols': len(headers) + 1,
            'content_hash': self.type1b_extractor._generate_hash(headers, extracted_data),
            'raw_html': str(table)
        }

    def _extract_single_table_type2(self, table, idx: int, filing_id: str, soup) -> Dict:
        """Extract a single Type 2 table using exact working commit logic"""
        rows = table.find_all('tr')
        if len(rows) < 2:
            return None

        # Use EXACT extraction logic from working commit 9ae5ef5
        headers = self.type2_extractor._extract_table_headers_simple(table)
        extracted_data = self.type2_extractor._extract_table_data_simple(table)

        # Apply Type 2 transformation using exact logic from working commit
        transformation = self.type2_extractor._transform_type2_table_data(extracted_data, headers)
        extracted_data = transformation['transformed_data']
        headers = transformation['new_headers']

        title = self.type2_extractor.extract_table_title(table, soup)

        return {
            'table_index': idx,
            'filing_id': filing_id,
            'title': title,
            'table_type': 'type_2',
            'headers': headers,
            'extracted_data': extracted_data,
            'num_rows': len(extracted_data),
            'num_cols': len(headers),
            'content_hash': self.type2_extractor._generate_hash(headers, extracted_data),
            'raw_html': str(table)
        }

    def extract_type1a_tables(self, html_content: str, filing_id: str) -> List[Dict]:
        """Extract only Type 1A tables"""
        return self.type1a_extractor.process_filing(html_content, filing_id)

    def extract_type1b_tables(self, html_content: str, filing_id: str) -> List[Dict]:
        """Extract only Type 1B tables"""
        return self.type1b_extractor.process_filing(html_content, filing_id)

    def extract_type2_tables(self, html_content: str, filing_id: str) -> List[Dict]:
        """Extract only Type 2 tables"""
        return self.type2_extractor.process_filing(html_content, filing_id)
