import hashlib
from typing import List, Dict
from bs4 import BeautifulSoup, Tag
from .base_extractor import BaseTableExtractor


class Type1A_Extractor(BaseTableExtractor):
    """Handles ONLY Type 1A tables (date headers in columns)"""

    def process_filing(self, html_content: str, filing_id: str) -> List[Dict]:
        """Legacy method for backward compatibility - not used in new architecture"""
        # This method is kept for backward compatibility but is not used
        # in the new TableExtractionManager-based architecture
        return []

    def _extract_type1a_headers(self, table: Tag) -> List[str]:
        """Type 1A: Extract date headers from column headers, skip metric column"""
        rows = table.find_all('tr')
        if len(rows) < 1:
            return []

        # Look for the header row with dates in columns 2+
        for row in rows[:5]:  # Check first 5 rows
            cells = row.find_all(['td', 'th'])
            if len(cells) > 1:  # Multi-column row
                date_headers = []
                for cell in cells[1:]:  # Skip first column (metric column)
                    text = cell.get_text(strip=True)
                    if text and self._has_date_pattern(text) and not self._is_metadata_cell(text):
                        date_headers.append(text)

                if date_headers:  # Found date headers
                    return date_headers

        return []

    def _extract_type1a_data(self, table: Tag, headers: List[str]) -> List[List[Dict]]:
        """Type 1A: Standard row-by-row extraction with date headers"""
        data_rows = []
        current_section = "default"

        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')

        for row_idx, row in enumerate(rows):
            # Check if this row is a section header
            section_header = self._is_section_header(row)
            if section_header:
                current_section = section_header
                # Add section header as a special row
                data_rows.append([{
                    'text': section_header,
                    'coordinates': {'row': row_idx, 'col': 0},
                    'is_section_header': True,
                    'section_context': current_section
                }])
                continue

            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            # Process regular data row
            row_data = []
            for col_idx, cell in enumerate(cells):
                span = cell.find('span')
                if span:
                    text = span.get_text(strip=True)
                else:
                    text = cell.get_text(strip=True)

                cell_data = {
                    'text': text,
                    'coordinates': {'row': row_idx, 'col': col_idx},
                    'is_section_header': False,
                    'section_context': current_section
                }
                row_data.append(cell_data)

            # Only include rows with actual content
            if row_data and any(cell['text'].strip() for cell in row_data):
                data_rows.append(row_data)

        return data_rows
