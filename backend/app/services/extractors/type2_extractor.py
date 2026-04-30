import hashlib
import re
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup, Tag
from .base_extractor import BaseTableExtractor


class Type2_Extractor(BaseTableExtractor):
    """Handles ONLY Type 2 tables using exact logic from commit 9ae5ef57439253af5e10751cb206bfdd03c01c54"""

    def process_filing(self, html_content: str, filing_id: str) -> List[Dict]:
        """Legacy method for backward compatibility - not used in new architecture"""
        # This method is kept for backward compatibility but is not used
        # in the new TableExtractionManager-based architecture
        return []

    def _extract_table_headers_simple(self, table: Tag) -> List[str]:
        """Extract segment headers for Type 2 tables"""
        rows = table.find_all('tr')

        # Find the row with segment headers (should be around row 1 or 2)
        for row in rows[1:4]:  # Check rows 1-3
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 8:  # Should have multiple columns
                headers = ['Metric']  # Start with metric column

                # Skip first cell (usually empty or "(in millions)"), then extract segment names
                for cell in cells[1:]:
                    text = cell.get_text(strip=True)
                    if text and text not in ['(in millions)', '(in thousands)', '']:
                        # Normalize non-breaking spaces and handle line breaks
                        text = text.replace('\xa0', ' ').replace('\n', ' ').replace('\r', ' ').strip()
                        # Ensure "Corporate and Eliminations" has proper spacing
                        if 'Corporate' in text and 'Eliminations' in text:
                            text = 'Corporate and Eliminations'
                        headers.append(text)

                if len(headers) >= 3:  # Should have at least Metric + 2 segments
                    return headers

        # Fallback: return basic headers
        return ['Metric', 'UnitedHealthcare', 'Optum Health', 'Optum Insight', 'Optum Rx', 'Optum Eliminations', 'Optum', 'Corporate and Eliminations', 'Consolidated']

    def _extract_table_data_simple(self, table: Tag) -> List[List[Dict]]:
        """Extract table data using EXACT logic from commit 9ae5ef57439253af5e10751cb206bfdd03c01c54"""
        data_rows = []
        current_section = "default"

        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')

        for row_idx, row in enumerate(rows):
            # Check if this row is a section header (single cell)
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

            # Check if this row is a date header spanning multiple columns
            date_header = self._is_date_header_row(row)
            if date_header:
                current_section = date_header
                # Add date header as a special row
                data_rows.append([{
                    'text': date_header,
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

    def _transform_type2_table_data(self, extracted_data: List[List[Dict]], headers: List[str]) -> Dict:
        """Transform Type 2 table to metric×date format"""

        # Find all date sections and their data
        date_sections = self._find_date_sections(extracted_data)

        # Extract segment headers (skip metric column)
        segment_headers = headers[1:] if len(headers) > 1 else []

        # Transform to metric × date format
        return self._create_metric_date_matrix(date_sections, segment_headers)

    def _find_date_sections(self, extracted_data: List[List[Dict]]) -> List[Dict]:
        """Find all date sections and their associated data rows"""
        date_sections = []
        current_date = None
        current_section = "default"
        current_rows = []

        for row_idx, row in enumerate(extracted_data):
            if len(row) == 1 and row[0].get('is_section_header', False):
                header_text = row[0]['text'].strip()

                # Check if this is a date header (spans multiple columns with date text)
                if self._is_date_header_text(header_text):
                    # Save previous section if exists
                    if current_date and current_rows:
                        date_sections.append({
                            'date': current_date,
                            'section': current_section,
                            'rows': current_rows
                        })

                    # Start new date section
                    current_date = header_text
                    current_rows = []
                else:
                    # This is a section header within the date period
                    current_section = header_text.rstrip(':')
            else:
                # Regular data row - add to current date section
                if current_date:
                    current_rows.append(row)

        # Don't forget the last section
        if current_date and current_rows:
            date_sections.append({
                'date': current_date,
                'section': current_section,
                'rows': current_rows
            })
        return date_sections

    def _create_metric_date_matrix(self, date_sections: List[Dict], segment_headers: List[str]) -> Dict:
        """Create metric × date matrix from date sections"""
        transformed_rows = []
        date_headers = [section['date'] for section in date_sections]

        for section in date_sections:
            date_header = section['date']
            section_name = section['section']

            for row in section['rows']:
                if len(row) <= 1:
                    continue

                # First cell is metric name
                metric_cell = row[0]
                metric_name = metric_cell.get('text', '').strip()

                if not metric_name or metric_name in ['(in millions)', '(in thousands)']:
                    continue

                # Extract values for each segment from the complex table structure
                # Different metrics use different patterns - detect which one to use

                # First, try the $ pattern (used by metrics like Premiums)
                dollar_values = []
                i = 1
                while i < len(row) and len(dollar_values) < len(segment_headers):
                    cell_text = row[i].get('text', '').strip()
                    if cell_text == '$' and i + 1 < len(row):
                        value = row[i + 1].get('text', '').strip()
                        dollar_values.append(value)
                        i += 4  # Skip $, value, empty, empty
                    else:
                        i += 1

                # Then, try the 3-cell pattern (used by metrics like Products)
                cell_values = []
                for segment_idx in range(len(segment_headers)):
                    value_pos = 2 + (segment_idx * 3)  # 2, 5, 8, 11, 14, 17, 20, 23
                    if value_pos < len(row):
                        value = row[value_pos].get('text', '').strip()
                        cell_values.append(value)
                    else:
                        cell_values.append('')

                # Choose the better extraction - prefer the one with more actual values (not empty/'—')
                dollar_score = sum(1 for v in dollar_values if v and v not in ['—', ''])
                cell_score = sum(1 for v in cell_values if v and v not in ['—', ''])

                used_dollar_pattern = False
                if dollar_score >= cell_score and dollar_values:
                    values = dollar_values
                    used_dollar_pattern = True
                else:
                    values = cell_values
                    used_dollar_pattern = False

                # Map values to segments
                for segment_idx, value in enumerate(values):
                    if segment_idx < len(segment_headers):
                        segment_name = segment_headers[segment_idx]

                        # Get the original coordinates of the cell that contains this value
                        # For $ pattern: value is at i+1, coordinates from row[i+1]
                        # For 3-cell pattern: value is at value_pos, coordinates from row[value_pos]
                        original_coords = {'row': 0, 'col': 0}  # fallback
                        if used_dollar_pattern:
                            # $ pattern was used - find the original cell coordinates
                            i = 1
                            value_count = 0
                            while i < len(row) and value_count <= segment_idx:
                                cell_text = row[i].get('text', '').strip()
                                if cell_text == '$' and i + 1 < len(row):
                                    if value_count == segment_idx:
                                        # This is the cell we want coordinates from
                                        original_coords = row[i + 1].get('coordinates', {'row': 0, 'col': 0})
                                        break
                                    value_count += 1
                                    i += 4
                                else:
                                    i += 1
                        else:
                            # 3-cell pattern was used
                            value_pos = 2 + (segment_idx * 3)
                            if value_pos < len(row):
                                original_coords = row[value_pos].get('coordinates', {'row': 0, 'col': 0})

                        # Create full metric name with section
                        full_metric_name = f"{section_name} :: {metric_name} :: {segment_name}"

                        # Find existing row for this metric or create new one
                        existing_row = None
                        for r in transformed_rows:
                            if r[0]['text'] == full_metric_name:
                                existing_row = r
                                break

                        if existing_row:
                            # Update the value for this date
                            date_idx = date_headers.index(date_header)
                            existing_row[date_idx + 1]['text'] = value  # +1 because first column is metric
                            # Update coordinates to the original cell coordinates
                            existing_row[date_idx + 1]['coordinates'] = original_coords
                        else:
                            # Create new row with empty values for all dates
                            new_row = [{
                                'text': full_metric_name,
                                'coordinates': {'row': len(transformed_rows), 'col': 0},
                                'is_section_header': False
                            }]

                            # Add empty values for all date periods
                            for dh_idx, dh in enumerate(date_headers):
                                cell_value = value if dh_idx == date_headers.index(date_header) else ''
                                cell_coords = original_coords if dh_idx == date_headers.index(date_header) else {'row': len(transformed_rows), 'col': len(new_row)}
                                new_row.append({
                                    'text': cell_value,
                                    'coordinates': cell_coords,
                                    'is_section_header': False
                                })

                            transformed_rows.append(new_row)

        return {
            'transformed_data': transformed_rows,
            'new_headers': ['Metric'] + date_headers
        }

    def _is_date_header_text(self, text: str) -> bool:
        """Check if text represents a date header"""
        if not text:
            return False

        text_lower = text.lower()

        # Check for "X months ended" pattern
        if 'months ended' not in text_lower:
            return False

        # Use regex to check for any month + any year combination
        month_pattern = r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b'
        year_pattern = r'\b\d{4}\b'

        has_month = bool(re.search(month_pattern, text, re.IGNORECASE))
        has_year = bool(re.search(year_pattern, text))

        return has_month and has_year

    def _group_rows_by_section_and_date(self, extracted_data: List[List[Dict]]) -> Dict[Tuple[str, str], List[List[Dict]]]:
        """Group rows by section and date headers"""
        sections_and_dates = {}
        current_section = "default"
        current_date = None

        for row in extracted_data:
            # Check if this is a section header
            if (len(row) >= 1 and
                row[0].get('is_section_header', False) and
                row[0].get('text', '').strip()):

                header_text = row[0]['text'].strip()

                # Check if this is a date header
                if 'months ended' in header_text.lower():
                    current_date = header_text
                else:
                    # This is a section header - strip trailing colon
                    current_section = header_text.rstrip(':')
            else:
                # Regular data row - add to current section and date
                if current_date:
                    key = (current_section, current_date)
                    if key not in sections_and_dates:
                        sections_and_dates[key] = []
                    sections_and_dates[key].append(row)

        return sections_and_dates

    def _group_rows_by_section(self, extracted_data: List[List[Dict]]) -> Dict[str, List[List[Dict]]]:
        """Group rows by section headers"""
        sections = {"default": []}
        current_section = "default"

        for row in extracted_data:
            # Check if this is a section header
            if (len(row) >= 1 and
                row[0].get('is_section_header', False) and
                row[0].get('text', '').strip()):

                header_text = row[0]['text'].strip()

                # Skip date headers, they are handled separately
                if 'months ended' in header_text.lower():
                    continue

                # This is a section header
                current_section = header_text
                sections[current_section] = []
            else:
                # Regular data row
                sections[current_section].append(row)

        return sections

    def _is_segment_based_table(self, extracted_data: List[List[Dict]]) -> bool:
        """Check if this is a segment-based table with UnitedHealthcare, Optum Health, etc."""
        for row in extracted_data:
            if len(row) > 5:  # Multi-column row
                # Look for segment names
                row_text = ' '.join([cell.get('text', '') for cell in row])
                segment_indicators = ['unitedhealthcare', 'optum health', 'optum insight', 'optum rx', 'consolidated']
                found_segments = sum(1 for segment in segment_indicators if segment.lower() in row_text.lower())
                if found_segments >= 3:  # Found multiple segments
                    return True
        return False

    def _transform_segment_based_table(self, extracted_data: List[List[Dict]], headers: List[str]) -> Dict:
        """Transform segment-based Type 2 table (like Optum table)"""

        # Find segment headers and date sections
        segment_headers = []
        date_sections = []

        current_date_section = None
        segment_row_found = False

        for row in extracted_data:
            if len(row) == 1 and row[0].get('is_section_header', False):
                # This is a date section header
                header_text = row[0]['text'].strip()
                if 'ended' in header_text.lower():
                    current_date_section = header_text
                    date_sections.append({'date': header_text, 'rows': []})
            elif not segment_row_found and len(row) > 5:
                # Check if this looks like segment headers
                row_text = ' '.join([cell.get('text', '') for cell in row])
                if 'unitedhealthcare' in row_text.lower() and 'optum' in row_text.lower():
                    # Extract segment names
                    for cell in row:
                        text = cell.get('text', '').strip()
                        if text and text not in ['(in millions)', '']:
                            segment_headers.append(text)
                    segment_row_found = True
            elif current_date_section and len(row) > 1:
                # This is data for the current date section
                if date_sections:
                    date_sections[-1]['rows'].append(row)

        # Extract clean date headers
        date_headers = []
        for section in date_sections:
            date_text = section['date']
            # Extract clean date (e.g., "June 30, 2025" from "Three Months Ended June 30, 2025")
            match = re.search(r'(?:three|six|nine|twelve)\s+months?\s+ended\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})', date_text, re.IGNORECASE)
            if match:
                clean_date = match.group(1)
            else:
                clean_date = date_text
            date_headers.append(clean_date)

        # Transform data: create metric × date matrix using consolidated values
        transformed_rows = []
        metric_data = {}

        for section_idx, section in enumerate(date_sections):
            date_header = date_headers[section_idx]

            for row in section['rows']:
                if len(row) <= 1:
                    continue

                # First cell should be metric name
                metric_cell = row[0]
                metric = metric_cell.get('text', '').strip()
                if not metric or metric in ['(in millions)', '']:
                    continue

                # Find consolidated value - use the last meaningful cell (Consolidated column)
                value = ''
                for cell in reversed(row[1:]):  # Skip metric column, go backwards to find last meaningful value
                    cell_text = cell.get('text', '').strip()
                    if cell_text and cell_text not in ['$', '—', '']:
                        value = cell_text
                        break

                if metric not in metric_data:
                    metric_data[metric] = {}

                if value:
                    metric_data[metric][date_header] = value

        # Convert to row format
        for metric, date_values in metric_data.items():
            row = [{
                'text': metric,
                'coordinates': {'row': len(transformed_rows), 'col': 0},
                'is_section_header': False,
                'section_context': 'default'
            }]

            # Add values for each date header
            for date_header in date_headers:
                value = date_values.get(date_header, '')
                row.append({
                    'text': value,
                    'coordinates': {'row': len(transformed_rows), 'col': len(row)},
                    'is_section_header': False,
                    'section_context': 'default'
                })

            transformed_rows.append(row)

        return {
            'transformed_data': transformed_rows,
            'new_headers': ['Metric'] + date_headers
        }

    def _transform_traditional_type2_table(self, extracted_data: List[List[Dict]], headers: List[str]) -> Dict:
        """Transform Type 2 table data using EXACT logic from commit 9ae5ef57439253af5e10751cb206bfdd03c01c54"""
        # Date patterns for identifying date sections (from working commit)
        date_patterns = [
            r'\b\d{1,2}/\d{1,2}/\d{4}\b',  # MM/DD/YYYY
            r'\b\d{1,2}-\d{1,2}-\d{4}\b',   # MM-DD-YYYY
            r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',  # Month DD, YYYY
            r'\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{4}\b',  # DD Mon YYYY
            r'\b(?:three|six|nine|twelve)\s+months?\s+ended\b',  # "Three months ended"
            r'\byear\s+ended\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',
            r'\b(?:three|six|nine|twelve)\s+months?\s+ended\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',
        ]

        # Group data by date section headers (from working commit)
        # First pass: identify all section headers
        all_section_headers = []
        for row in extracted_data:
            if (len(row) == 1 and
                row[0].get('is_section_header', False) and
                row[0].get('text', '').strip()):
                header_text = row[0]['text'].strip()
                all_section_headers.append(header_text)

        # Second pass: separate date sections from sub-sections (from working commit)
        date_sections = []
        current_date_section = None
        current_rows = []

        for row in extracted_data:
            if (len(row) == 1 and
                row[0].get('is_section_header', False) and
                row[0].get('text', '').strip()):

                header_text = row[0]['text'].strip()

                # Check if this is a date section header
                is_date_header = any(re.search(pattern, header_text.lower()) for pattern in date_patterns)

                if is_date_header:
                    # Save previous date section if it exists
                    if current_date_section and current_rows:
                        date_sections.append({
                            'date': current_date_section,
                            'rows': current_rows
                        })

                    # Start new date section
                    current_date_section = header_text
                    current_rows = []
                else:
                    # This is a sub-section within the current date section
                    # Add it as a row in the current section
                    if current_date_section:
                        current_rows.append(row)
            else:
                # Regular data row
                if current_date_section:
                    current_rows.append(row)

        # Don't forget the last section
        if current_date_section and current_rows:
            date_sections.append({
                'date': current_date_section,
                'rows': current_rows
            })

        # Extract clean date headers from section names (from working commit)
        date_headers = []

        # All sections in date_sections are now actual date sections
        # Also identify the table section from sub-section headers in the data
        table_section = None

        for section in date_sections:
            date_text = section['date']

            # Extract clean date header (from working commit)
            match = re.search(r'(?:three|six|nine|twelve)\s+months?\s+ended\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})', date_text, re.IGNORECASE)
            if match:
                clean_date = match.group(1)
            else:
                # Try to extract just the date part
                date_match = re.search(r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})', date_text, re.IGNORECASE)
                if date_match:
                    clean_date = date_match.group(1)
                else:
                    clean_date = date_text  # Fallback

            if clean_date not in date_headers:
                date_headers.append(clean_date)

            # Try to identify table section from sub-section headers (from working commit)
            for row in section['rows']:
                if len(row) > 0 and row[0].get('is_section_header', False):
                    sub_header = row[0]['text'].strip()
                    if sub_header and not any(re.search(pattern, sub_header.lower()) for pattern in date_patterns):
                        if not table_section:
                            table_section = sub_header
                        break

        # If no date sections found, return original data
        if not date_sections:
            return {
                'transformed_data': extracted_data,
                'new_headers': ['Metric'] + headers
            }

        # Transform data: create metric × date matrix (from working commit)
        transformed_rows = []

        # Group data by metric (first column) across all date sections
        metric_data = {}

        for section in date_sections:
            date_header = section['date']
            # Clean up date header
            match = re.search(r'(?:three|six|nine|twelve)\s+months?\s+ended\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})', date_header, re.IGNORECASE)
            if match:
                clean_date = match.group(1)
            else:
                date_match = re.search(r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})', date_header, re.IGNORECASE)
                if date_match:
                    clean_date = date_match.group(1)
                else:
                    clean_date = date_header

            # Process each row in this date section
            for row in section['rows']:
                if len(row) <= 1 or row[0].get('is_section_header', False):
                    continue  # Skip section headers and rows with no data

                metric = row[0]['text'].strip()
                if not metric:
                    continue

                # Initialize metric entry if not exists
                if metric not in metric_data:
                    metric_data[metric] = {}

                # Add data for each column after metric
                for col_idx, cell in enumerate(row[1:], 1):
                    if col_idx < len(headers) + 1:  # +1 for potential extra columns
                        value = cell['text'].strip()
                        if value:
                            metric_data[metric][clean_date] = value

        # Convert to row format
        for metric, date_values in metric_data.items():
            row = [{
                'text': metric,
                'coordinates': {'row': len(transformed_rows), 'col': 0},
                'is_section_header': False,
                'section_context': table_section or 'default'
            }]

            # Add values for each date header
            for date_header in date_headers:
                value = date_values.get(date_header, '')
                row.append({
                    'text': value,
                    'coordinates': {'row': len(transformed_rows), 'col': len(row)},
                    'is_section_header': False,
                    'section_context': table_section or 'default'
                })

            transformed_rows.append(row)

        return {
            'transformed_data': transformed_rows,
            'new_headers': ['Metric'] + date_headers
        }

    def _is_date_header_row(self, row: Tag) -> Optional[str]:
        """Check if a row is a date header that spans multiple columns."""
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 1:
            # Check if first cell contains date-like text and spans multiple columns
            first_cell = cells[0]
            text = first_cell.get_text(strip=True)

            # Date patterns
            date_patterns = [
                r'\b\d{1,2}/\d{1,2}/\d{4}\b',  # MM/DD/YYYY
                r'\b\d{1,2}-\d{1,2}-\d{4}\b',   # MM-DD-YYYY
                r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',  # Month DD, YYYY
                r'\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{4}\b',  # DD Mon YYYY
                r'\b(?:three|six|nine|twelve)\s+months?\s+ended\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',  # "Six Months Ended June 30, 2024"
            ]

            if any(re.search(pattern, text, re.IGNORECASE) for pattern in date_patterns):
                # Check if this cell spans most of the table (likely a date header)
                colspan = first_cell.get('colspan')
                if colspan:
                    try:
                        span_width = int(colspan)
                        # If it spans more than 10 columns, it's likely a date header
                        if span_width > 10:
                            return text
                    except ValueError:
                        pass
                # Also check if it's a meaningful date header even without colspan
                elif len(text) > 15 and 'ended' in text.lower():
                    return text

        return None
