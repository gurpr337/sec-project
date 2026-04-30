"""
Cloned Type1B_Extractor for table analysis - identical logic, separate from production extraction
"""
import hashlib
from typing import List, Dict, Optional
from bs4 import BeautifulSoup, Tag
from .analysis_base_extractor import AnalysisBaseExtractor


class AnalysisType1B_Extractor(AnalysisBaseExtractor):
    """Cloned Type 1B extractor for table analysis - preserves original header flattening logic"""

    def process_table_analysis(self, table: Tag, soup) -> Dict:
        """
        Process a table for analysis - cloned from Type1B_Extractor logic
        Returns analysis results including original HTML and parsed data
        """
        rows = table.find_all('tr')
        if len(rows) < 2:
            return None

        # Extract headers using Type 1B logic
        headers = self._extract_type1b_headers(table)

        # Add metrics column header for Type 1B tables
        headers.insert(0, "Metrics")

        # Extract data using Type 1B logic
        extracted_data = self._extract_type1b_data(table, headers)

        # Extract title
        title = self.extract_table_title(table, soup)

        # Generate content hash
        content_hash = self._generate_hash(headers, extracted_data)

        return {
            'original_html': str(table),
            'title': title,
            'headers': headers,
            'extracted_data': extracted_data,
            'num_rows': len(extracted_data),
            'num_cols': len(headers) + 1,
            'content_hash': content_hash
        }

    def _extract_type1b_headers(self, table: Tag) -> List[str]:
        """Type 1B: Use hierarchical flattening for multi-row date headers - cloned logic"""
        all_header_rows = self._identify_header_rows_for_simple(table)
        if len(all_header_rows) > 1:
            column_headers = self._build_column_hierarchy_from_rows(all_header_rows)
            return [header['flattened_name'] for header in column_headers if header.get('flattened_name')]
        return []

    def _build_column_hierarchy_from_rows(self, header_rows: List[Tag]) -> List[Dict]:
        """Build column hierarchy from header rows with proper flattening - cloned logic"""
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
        """Extract date header from flattened parts, ignoring period descriptors - cloned logic"""
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
        """Type 1B: Standard extraction with proper logical column mapping - cloned logic"""
        data_rows = []
        current_section = "default"

        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')

        # Get the logical column mapping from header processing
        # For Type 1B tables, we need to identify header rows more carefully
        # Header rows contain hierarchical date structures, not data
        header_rows = self._identify_header_rows_for_simple(table)

        # Filter header rows to only include those with actual hierarchical date patterns
        actual_header_rows = []
        for row in header_rows:
            if self._is_actual_header_row(row):
                actual_header_rows.append(row)

        column_mapping = []
        if actual_header_rows:
            column_info = self._build_column_hierarchy_from_rows(actual_header_rows)
            column_mapping = [(col['col_idx'], col['colspan']) for col in column_info]

        # Process all rows as potential data rows
        # Skip only the actual header rows, not data rows that happen to have content
        for row_idx, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            # Extract text from first cell to determine row type
            first_cell = cells[0]
            span = first_cell.find('span')
            if span:
                first_cell_text = span.get_text(strip=True)
            else:
                first_cell_text = first_cell.get_text(strip=True)

            # Skip actual header rows (rows with hierarchical date structures)
            # Data rows contain metrics and numbers, header rows contain date hierarchies
            is_actual_header_row = row in actual_header_rows
            if is_actual_header_row:
                continue

            # Check if this row is a section header
            section_header = self._is_section_header(row)
            if section_header:
                current_section = section_header
                # Add section header as a special row - create full width to match headers
                section_row = []
                for col_idx in range(len(headers)):
                    section_row.append({
                        'text': section_header if col_idx == 0 else '',
                        'coordinates': {'row': row_idx, 'col': col_idx},
                        'is_section_header': True,
                        'section_context': current_section
                    })
                data_rows.append(section_row)
                continue

            # This is a data row - extract metrics and data values
            # Simple approach: metrics in first column, data in subsequent columns
            logical_row = [first_cell_text]  # Start with metrics

            # Extract data values from remaining cells
            for cell in cells[1:]:  # Skip the metrics cell
                span = cell.find('span')
                if span:
                    text = span.get_text(strip=True)
                else:
                    text = cell.get_text(strip=True)

                # Add any cell with content
                if text.strip():
                    logical_row.append(text)

            # Ensure row has the expected number of columns
            while len(logical_row) < len(headers):
                logical_row.append('')

            # Truncate if too many columns
            if len(logical_row) > len(headers):
                logical_row = logical_row[:len(headers)]

            # Convert to proper format
            formatted_row = []
            for col_idx, cell_text in enumerate(logical_row):
                formatted_row.append({
                    'text': cell_text,
                    'coordinates': {'row': row_idx, 'col': col_idx},
                    'is_section_header': False,
                    'section_context': current_section
                })

            # Only include rows with actual content in data columns
            data_cells = formatted_row[1:] if formatted_row else []
            if formatted_row and any(cell['text'].strip() for cell in data_cells):
                data_rows.append(formatted_row)

        return data_rows

    def _is_actual_header_row(self, row: Tag) -> bool:
        """Check if a row is an actual header row with hierarchical date structures"""
        cells = row.find_all(['td', 'th'])
        if not cells:
            return False

        # Header rows typically have multiple cells with date-related content
        # or hierarchical structures spanning multiple columns
        date_related_cells = 0
        total_cells = len(cells)

        for cell in cells:
            text = cell.get_text(strip=True)
            if not text:
                continue

            # Check if cell contains date-related patterns
            if (self._has_date_pattern(text) or
                self._is_date_header_text(text) or
                'ended' in text.lower() or
                'months' in text.lower() or
                'year' in text.lower()):
                date_related_cells += 1

        # Consider it a header row if more than 30% of cells have date content
        # or if it has hierarchical colspan structure
        has_hierarchical_structure = any(int(cell.get('colspan', 1)) > 2 for cell in cells)

        return date_related_cells >= total_cells * 0.3 or has_hierarchical_structure

    def _is_section_header(self, row: Tag) -> Optional[str]:
        """Check if a row is a section header - cloned logic"""
        cells = row.find_all(['td', 'th'])
        if not cells:
            return None

        # Condition: First cell has text, all other cells are empty
        first_cell_text = cells[0].get_text(strip=True)
        if not first_cell_text:
            return None

        # Check if all other cells are empty
        for cell in cells[1:]:
            if cell.get_text(strip=True):
                return None  # Not a section header if other cells have content

        # Skip obvious non-section content
        if first_cell_text in ['☒', '☐', '*']:
            return None

        # Skip single characters
        if len(first_cell_text) < 2:
            return None

        return first_cell_text

    def _generate_hash(self, headers: List[str], data: List[List[Dict]]) -> str:
        """Generate content hash for deduplication - cloned logic"""
        content = f"{headers}{data}"
        return hashlib.md5(content.encode()).hexdigest()
