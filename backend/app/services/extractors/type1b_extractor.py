import hashlib
from typing import List, Dict, Optional
from bs4 import BeautifulSoup, Tag
from .base_extractor import BaseTableExtractor


class Type1B_Extractor(BaseTableExtractor):
    """Handles ONLY Type 1B tables (hierarchical date headers)"""

    def process_filing(self, html_content: str, filing_id: str) -> List[Dict]:
        """Legacy method for backward compatibility - not used in new architecture"""
        # This method is kept for backward compatibility but is not used
        # in the new TableExtractionManager-based architecture
        return []

    def _extract_type1b_headers(self, table: Tag) -> List[str]:
        """Type 1B: Use hierarchical flattening for multi-row date headers"""
        all_header_rows = self._identify_header_rows_for_simple(table)
        if len(all_header_rows) > 1:
            column_headers = self._build_column_hierarchy_from_rows(all_header_rows)
            return [header['flattened_name'] for header in column_headers if header.get('flattened_name')]
        return []

    def _build_column_hierarchy_from_rows(self, header_rows: List[Tag]) -> List[Dict]:
        """Build column hierarchy from header rows with proper flattening"""
        if not header_rows:
            return []

        # First pass: identify actual data columns by finding the row with date headers
        # This is typically not the year row, but the row with actual date text
        bottom_row = None
        for row in reversed(header_rows):
            cells = row.find_all(['td', 'th'])
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            # Check if this row has date-like content (but not just years)
            has_date_content = any(
                text and len(text) > 4 and  # Not just years
                ('ended' in text.lower() or 'june' in text.lower() or 'december' in text.lower() or ',' in text)
                for text in cell_texts
            )
            if has_date_content:
                bottom_row = row
                break

        if not bottom_row:
            # Fallback to last row
            bottom_row = header_rows[-1]

        bottom_cells = bottom_row.find_all(['td', 'th'])

        # Identify logical data columns (accounting for colspan)
        data_column_groups = []
        c_idx = 0
        for cell in bottom_cells:
            raw_name = cell.get_text(strip=True)
            colspan = int(cell.get('colspan', 1))

            # Skip metadata cells (units, notes, etc.)
            if self._is_metadata_cell(raw_name):
                c_idx += colspan
                continue

            # This is a logical data column group
            data_column_groups.append({
                'start_col': c_idx,
                'colspan': colspan,
                'raw_name': raw_name
            })
            c_idx += colspan

        if not data_column_groups:
            # Fallback: assume all columns in bottom row are data columns
            c_idx = 0
            for cell in bottom_cells:
                colspan = int(cell.get('colspan', 1))
                raw_name = cell.get_text(strip=True)
                if not self._is_metadata_cell(raw_name):
                    data_column_groups.append({
                        'start_col': c_idx,
                        'colspan': colspan,
                        'raw_name': raw_name
                    })
                c_idx += colspan

        # Second pass: build flattened headers for logical data columns
        column_headers = []
        header_id_counter = 0

        for group in data_column_groups:
            # Build the flattened name by traversing up the hierarchy
            flattened_parts = []

            for row_idx, row in enumerate(header_rows):
                cells = row.find_all(['td', 'th'])
                current_col = 0

                for cell in cells:
                    cell_colspan = int(cell.get('colspan', 1))
                    cell_rowspan = int(cell.get('rowspan', 1))

                    # Check if this cell spans the target column group
                    if current_col <= group['start_col'] < current_col + cell_colspan:
                        raw_name = cell.get_text(strip=True)
                        # Include years even if they're considered metadata
                        if raw_name and (not self._is_metadata_cell(raw_name) or (len(raw_name.strip()) == 4 and raw_name.strip().isdigit())):
                            flattened_parts.append(raw_name)
                        break

                    current_col += cell_colspan

            # Create the final flattened header
            if flattened_parts:
                # For hierarchical headers, use the full flattened name
                # Only use date extraction if we have a simple date string
                if len(flattened_parts) == 1:
                    # Single part - might be a simple date
                    flattened_name = self._extract_date_header(flattened_parts)
                    if not flattened_name:
                        flattened_name = flattened_parts[0]
                else:
                    # Multiple parts - hierarchical, use full joined name
                    flattened_name = ' '.join(flattened_parts).strip()

                column_header = {
                    'id': header_id_counter,
                    'raw_name': flattened_parts[-1] if flattened_parts else '',
                    'level': len(flattened_parts) - 1,
                    'col_idx': group['start_col'],
                    'colspan': group['colspan'],
                    'rowspan': 1,
                    'flattened_name': flattened_name,
                    'parent_id': None
                }
                column_headers.append(column_header)
                header_id_counter += 1

        return column_headers

    def _extract_date_header(self, flattened_parts: List[str]) -> Optional[str]:
        """Extract date header from flattened parts, ignoring period descriptors"""
        if not flattened_parts:
            return None

        # Combine all parts for analysis
        full_text = ' '.join(flattened_parts).strip()

        # Find date patterns in the text (most specific first)
        import re
        date_patterns = [
            r'(\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*\d{4}\b)',  # Full date
            r'(\b\d{1,2}/\d{1,2}/\d{4}\b)',  # MM/DD/YYYY
            r'(\b\d{4}-\d{2}-\d{2}\b)',  # YYYY-MM-DD
        ]

        for pattern in date_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        # Handle the specific SEC table pattern: "Month DD," + "YYYY"
        # Look for month/day in any part and year in the last part
        month_day = None
        year = None

        # First, look for month/day pattern in any part
        for part in flattened_parts:
            month_match = re.search(r'\b((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?)', part, re.IGNORECASE)
            if month_match:
                month_day = month_match.group(1).rstrip(',')
                break

        # Look for year pattern (prefer the last part, which is often just the year)
        for part in reversed(flattened_parts):
            year_match = re.search(r'\b(\d{4})\b', part.strip())
            if year_match:
                year = year_match.group(1)
                break

        # If we found both month/day and year, combine them
        if month_day and year:
            return f"{month_day}, {year}"

        return None

    def _extract_type1b_data(self, table: Tag, headers: List[str]) -> List[List[Dict]]:
        """Type 1B: Standard extraction (headers already flattened)"""
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
