import hashlib
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup, Tag


class BaseTableExtractor:
    """Common functionality for all table extractors"""

    def __init__(self):
        self.user_agent = "SECExtractor/1.0 (contact@example.com)"

    def extract_table_title(self, table: Tag, soup: BeautifulSoup) -> str:
        """Extract table title from surrounding context with improved detection"""
        candidates = []

        # Method 1: Check for caption
        caption = table.find('caption')
        if caption:
            caption_text = caption.get_text(strip=True)
            if caption_text and len(caption_text) > 3:
                return caption_text

        # Method 2: Look for previous sibling elements (paragraphs, headings)
        prev = table.find_previous_sibling()
        attempts = 0

        while prev and attempts < 10:
            if prev.name in ['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                text = prev.get_text(strip=True)
                if text and len(text) > 3 and len(text) < 300:
                    # Check if it looks like a title (not too numeric, not too short)
                    word_count = len(text.split())
                    if word_count >= 1 and word_count <= 20:
                        # Avoid titles that are mostly numbers or codes
                        alpha_ratio = sum(1 for c in text if c.isalpha()) / len(text) if text else 0
                        if alpha_ratio > 0.3:  # At least 30% alphabetic characters
                            candidates.append(text)

            prev = prev.find_previous_sibling()
            attempts += 1

        # Method 3: Look for parent container with title-like text
        parent = table.parent
        if parent and parent.name in ['div', 'section']:
            for child in parent.find_all(['p', 'span', 'div']):
                if child != table:  # Don't include the table itself
                    text = child.get_text(strip=True)
                    if text and len(text) > 3 and len(text) < 200:
                        word_count = len(text.split())
                        if word_count >= 2 and word_count <= 15:
                            alpha_ratio = sum(1 for c in text if c.isalpha()) / len(text) if text else 0
                            if alpha_ratio > 0.4:
                                candidates.append(text)

        # Filter and rank candidates
        valid_candidates = []
        for candidate in candidates:
            # Remove extra whitespace and normalize
            candidate = ' '.join(candidate.split())
            if len(candidate) > 3 and len(candidate) < 200:
                # Avoid candidates that are just numbers or codes
                if not candidate.replace('.', '').replace('-', '').replace('/', '').isdigit():
                    # Avoid generic terms that aren't good titles
                    lower_candidate = candidate.lower()
                    if not any(phrase in lower_candidate for phrase in [
                        'the company', 'the following', 'see note', 'see accompanying',
                        'in millions', 'in thousands', 'except percentages'
                    ]):
                        valid_candidates.append(candidate)

        # Return the best candidate
        if valid_candidates:
            # Prefer longer, more descriptive titles
            valid_candidates.sort(key=len, reverse=True)
            return valid_candidates[0]

        return "Untitled Table"

    def _has_date_pattern(self, text: str) -> bool:
        """Check if text contains actual date patterns (not period descriptions)"""
        if not text:
            return False

        text_lower = text.lower()

        # Look for actual date formats, not just period descriptions
        # Month DD, YYYY format
        if re.search(r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\s+\d{1,2},?\s*\d{4}\b', text_lower):
            return True

        # DD/MM/YYYY or similar
        if re.search(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b', text):
            return True

        # YYYY-MM-DD
        if re.search(r'\b\d{4}-\d{2}-\d{2}\b', text):
            return True

        # "months ended" + specific date (but not just "months")
        if 'months ended' in text_lower:
            # Must have a specific month and year
            has_specific_month = any(month in text_lower for month in ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'])
            has_year = bool(re.search(r'\b\d{4}\b', text))
            if has_specific_month and has_year:
                return True

        return False

    def _determine_table_type(self, table: Tag) -> str:
        """Analyze table structure to determine type BEFORE any extraction"""
        rows = table.find_all('tr')
        if len(rows) < 2:
            return 'unknown'

        # Check first 4 rows for date patterns in data columns (Type 1A)
        for row in rows[:4]:
            cells = row.find_all(['td', 'th'])
            if len(cells) > 1:  # Multi-column row
                # Count date headers in this row
                date_headers_in_row = 0
                total_data_cells = 0

                for cell in cells[1:]:  # Skip first column (metrics)
                    text = cell.get_text(strip=True)
                    if text.strip():  # Non-empty cell
                        total_data_cells += 1
                        if self._has_date_pattern(text):
                            date_headers_in_row += 1

                # If this row has mostly date headers (at least 2, and >50% of cells), it's Type 1A
                if date_headers_in_row >= 2 and date_headers_in_row >= total_data_cells * 0.5:
                    return 'type_1a'  # Simple date header row

        # Check for multi-row header patterns (Type 1B) - BEFORE Type 2 detection
        header_rows = self._identify_header_rows_for_simple(table)
        has_hierarchical = len(header_rows) > 1 and self._has_hierarchical_date_patterns(header_rows)

        # Also check for tables that should be Type 1B based on content (fallback for complex tables)
        should_be_type1b = self._should_be_processed_as_type1b(table)

        if has_hierarchical or should_be_type1b:
            return 'type_1b'  # Multi-row date headers needing flattening

        # Check for Type 2 - tables with segment-based structure
        has_date_section_headers = False
        has_segment_headers = False

        for row in rows[:20]:  # Check first 20 rows
            cells = row.find_all(['td', 'th'])

            # Check for date headers anywhere in the table (more permissive)
            for cell in cells:
                text = cell.get_text(strip=True)
                if self._is_date_header_text(text) or self._has_date_pattern(text):
                    has_date_section_headers = True
                    break

            # Check for segment headers (more permissive)
            if len(cells) >= 3:  # Reduced from >5 to >=3
                segment_texts = []
                for cell in cells:
                    text = cell.get_text(strip=True)
                    if text and len(text) > 3 and not self._has_date_pattern(text) and not text.startswith('('):
                        segment_texts.append(text)
                if len(segment_texts) >= 2:  # Reduced from >=3 to >=2
                    has_segment_headers = True

        if has_date_section_headers and has_segment_headers:
            return 'type_2'

        return 'unknown'

    def _find_column_headers_row(self, table: Tag) -> Optional[Tag]:
        """Find the row that contains the main column headers"""
        rows = table.find_all('tr')

        # Skip first row (often table title)
        for row in rows[1:6]:  # Check first few rows
            cells = row.find_all(['td', 'th'])

            # Must have multiple cells
            if len(cells) >= 3:
                # Count cells that look like headers (short, non-numeric text)
                header_like = 0
                for cell in cells:
                    text = cell.get_text(strip=True)
                    # Headers are typically short text, not long numbers
                    if text and 2 <= len(text) <= 50:
                        # Not purely numeric
                        if not text.replace(',', '').replace('.', '').replace('$', '').replace('%', '').isdigit():
                            header_like += 1

                # If majority of cells look like headers
                if header_like >= len(cells) * 0.6:
                    return row

        return None

    def _is_potential_type2_table(self, table: Tag) -> bool:
        """Check if table might be Type 2: column headers don't contain dates"""

        # Find the column headers row
        header_row = self._find_column_headers_row(table)
        if not header_row:
            return False

        # Extract header texts
        header_texts = []
        for cell in header_row.find_all(['td', 'th']):
            text = cell.get_text(strip=True)
            if text and text.strip():
                header_texts.append(text)

        # If ANY column header contains dates = Type 1 (not Type 2)
        # If NO column headers contain dates = potential Type 2
        has_date_headers = any(self._has_date_pattern(header) for header in header_texts)

        return not has_date_headers  # Potential Type 2 if no dates in headers

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

    def _looks_like_segment(self, text: str) -> bool:
        """Check if text looks like a business segment name"""
        if not text or len(text.strip()) < 3:
            return False

        text_lower = text.lower().strip()

        # Segment indicators
        segment_patterns = [
            'unitedhealthcare', 'optum', 'optumhealth', 'optumrx', 'uhc',
            'domestic', 'international', 'commercial', 'medicare', 'medicaid',
            'individual', 'employer', 'group', 'senior', 'military',
            'americas', 'asia', 'europe', 'latin america', 'north america'
        ]

        # Check for segment patterns
        for pattern in segment_patterns:
            if pattern in text_lower:
                return True

        # Check for company-like structure (capitalized words, business terms)
        words = text.split()
        if len(words) >= 2:
            # Multiple capitalized words or business terms
            cap_words = sum(1 for word in words if word and word[0].isupper())
            if cap_words >= 2:
                return True

        # Check for business unit patterns
        business_indicators = ['group', 'division', 'segment', 'business', 'unit', 'operations']
        for indicator in business_indicators:
            if indicator in text_lower:
                return True

        return False

    def _confirm_type2_with_header_extraction(self, table: Tag) -> bool:
        """Extract headers using original commit logic and confirm Type 2"""
        # For Type 2 tables, extract segment headers (not date headers)
        headers = self._extract_type2_segment_headers(table)

        # Check if headers are segments (no dates) using original is_type2_table logic
        return self._is_type2_table_original_logic(headers)

    def _extract_type2_segment_headers(self, table: Tag) -> List[str]:
        """Extract segment headers for Type 2 tables (not date headers)"""
        rows = table.find_all('tr')

        # Look for the row with segment-like headers (skip first column which is metric)
        for row in rows[:10]:  # Check first 10 rows
            cells = row.find_all(['th', 'td'])
            if len(cells) > 1:  # Multi-column row
                segment_headers = []

                # Skip first cell (usually metric column)
                for cell in cells[1:]:
                    text = cell.get_text(strip=True)
                    if text and len(text.strip()) > 2:
                        # Check if this looks like a segment header (not metadata, not dates)
                        if not self._has_date_pattern(text) and not self._is_metadata_cell(text):
                            segment_headers.append(text.strip())

                # If we found multiple segment headers, this is likely the right row
                if len(segment_headers) >= 3:  # At least 3 segments
                    return segment_headers

        # Fallback: return empty list
        return []

    def _extract_headers_like_working_commit(self, table: Tag) -> List[str]:
        """Extract headers using exact logic from commit 9ae5ef57439253af5e10751cb206bfdd03c01c54"""
        import re

        # First, identify header rows
        header_rows = self._identify_header_rows_for_simple(table)

        if not header_rows:
            return []

        # Build flattened headers from hierarchical structure (from working commit)
        flattened_headers = []

        # Find the header row with the most date-like headers (from working commit)
        date_patterns = [
            r'\b\d{1,2}/\d{1,2}/\d{4}\b',  # MM/DD/YYYY
            r'\b\d{1,2}-\d{1,2}-\d{4}\b',   # MM-DD-YYYY
            r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',  # Month DD, YYYY
            r'\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{4}\b',  # DD Mon YYYY
            r'\b(?:three|six|nine|twelve)\s+months?\s+ended\b',  # "Three months ended", "Six months ended"
            r'\byear\s+ended\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',  # "Year ended December 31, 2024"
            r'\b(?:three|six|nine|twelve)\s+months?\s+ended\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',  # "Six Months Ended June 30, 2024"
        ]

        best_row = None
        max_date_count = 0

        for row in header_rows:
            cells = row.find_all(['th', 'td'])
            date_count = 0
            for cell in cells:
                span = cell.find('span')
                if span:
                    text = span.get_text(strip=True)
                else:
                    text = cell.get_text(strip=True)
                if any(re.search(pattern, text) for pattern in date_patterns):
                    date_count += 1
            if date_count > max_date_count:
                max_date_count = date_count
                best_row = row

        # If no row has dates, fall back to bottom row
        if best_row is None:
            best_row = header_rows[-1]

        # Get all cells in the best header row (from working commit logic)
        cells = best_row.find_all(['th', 'td'])

        for cell in cells:
            # Get text from spans within the cell (SEC tables often have text in spans)
            span = cell.find('span')
            if span:
                text = span.get_text(strip=True)
            else:
                text = cell.get_text(strip=True)

            # Skip empty cells or cells with only styling/spacers
            if not text or text.strip() == '' or len(text.strip()) < 2:
                continue

            # Skip cells that are just numbers or currency symbols, but allow meaningful headers
            cleaned_text = text.replace('.', '').replace(',', '').replace('$', '').replace('(', '').replace(')', '').replace('-', '').replace(' ', '')

            # Allow year headers (4-digit years like 2024, 2023)
            if cleaned_text.isdigit() and len(cleaned_text) == 4 and cleaned_text.startswith(('19', '20', '21')):
                # This is likely a year header, keep it
                pass
            elif cleaned_text.isdigit():
                # Skip other pure numeric cells
                continue

            # Handle colspan: if cell spans multiple columns, repeat the text
            # But only for meaningful hierarchical headers, not for formatting/layout
            colspan = cell.get('colspan')
            if colspan:
                try:
                    span_count = int(colspan)
                    # Only expand if colspan > 1 and text represents a meaningful header segment
                    # Check if this looks like a hierarchical header (contains time periods, financial terms, etc.)
                    meaningful_header_indicators = [
                        'months ended', 'year ended', 'quarter', 'period',
                        'three months', 'six months', 'nine months', 'twelve months',
                        'q1', 'q2', 'q3', 'q4', '1q', '2q', '3q', '4q'
                    ]
                    is_meaningful_header = (
                        span_count > 1 and
                        len(text.strip()) > 5 and  # Meaningful length
                        any(indicator.lower() in text.lower() for indicator in meaningful_header_indicators)
                    )

                    if is_meaningful_header:
                        for _ in range(span_count):
                            flattened_headers.append(text.strip())
                        continue
                except (ValueError, TypeError):
                    pass

            flattened_headers.append(text.strip())

        return flattened_headers

    def _is_type2_table_original_logic(self, headers: List[str]) -> bool:
        """Check if a table is Type 2 using exact logic from commit 9ae5ef57439253af5e10751cb206bfdd03c01c54"""
        import re

        # Comprehensive date patterns from working commit
        date_patterns = [
            # Full dates
            r'\b\d{1,2}/\d{1,2}/\d{4}\b',  # MM/DD/YYYY
            r'\b\d{1,2}-\d{1,2}-\d{4}\b',   # MM-DD-YYYY
            r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',  # Month DD, YYYY
            r'\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{4}\b',  # DD Mon YYYY

            # Period descriptions
            r'\b(?:three|six|nine|twelve)\s+months?\s+ended\b',  # "Three months ended"
            r'\byear\s+ended\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',
            r'\b(?:three|six|nine|twelve)\s+months?\s+ended\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',

            # Just years
            r'\b(19|20|21)\d{2}\b'
        ]

        # Check if any header contains a date pattern
        for header in headers:
            for pattern in date_patterns:
                if re.search(pattern, header.lower()):
                    return False  # Has dates = Type 1

        return True  # No dates in headers = Type 2

    def _identify_header_rows_for_simple(self, table: Tag) -> List[Tag]:
        """Identify header rows for simple extraction"""
        # First priority: rows in thead
        thead = table.find('thead')
        if thead:
            header_rows = thead.find_all('tr')
            if header_rows:
                return header_rows

        # Fallback: look for rows that appear to be headers in the first part of the table
        rows = table.find_all('tr')
        header_rows = []

        for i, row in enumerate(rows[:15]):  # Check first 15 rows for complex tables
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            # Count meaningful cells
            meaningful_cells = 0
            total_cells = len(cells)

            for cell in cells:
                span = cell.find('span')
                if span:
                    text = span.get_text(strip=True)
                else:
                    text = cell.get_text(strip=True)

                if text and text.strip():
                    cleaned_text = text.strip()
                    # Allow 4-digit years as meaningful (they're headers in hierarchical tables)
                    if len(cleaned_text) == 4 and cleaned_text.isdigit():
                        meaningful_cells += 1
                    # Not just numbers or single characters
                    elif len(cleaned_text) > 1:
                        cleaned = cleaned_text.replace('.', '').replace(',', '').replace('$', '').replace('(', '').replace(')', '').replace('-', '').replace(' ', '')
                        if not cleaned.isdigit() and len(cleaned) > 1:
                            meaningful_cells += 1

            # Check if this row contains date patterns (prioritize these)
            has_date_patterns = False
            for cell in cells:
                text = cell.get_text(strip=True)
                if self._has_date_pattern(text):
                    has_date_patterns = True
                    break

            # Check for year-only rows (important for hierarchical tables)
            has_years = any(len(cell.get_text(strip=True)) == 4 and cell.get_text(strip=True).isdigit() for cell in cells)

            # Consider it a header row if:
            # 1. Has date patterns (highest priority), or
            # 2. Has year patterns (important for hierarchical), or
            # 3. Has multiple meaningful cells, or
            # 4. Is in a thead, or
            # 5. First few rows (SEC tables often have complex headers)
            # But exclude rows that look like data (have actual numbers/values, but not years)
            has_numeric_data = any(
                text.strip() and
                len(text.strip()) != 4 and  # Exclude 4-digit years
                text.strip().replace(',', '').replace('.', '').replace('-', '').replace('—', '').replace('$', '').replace('%', '').isdigit()
                for cell in cells
                for text in [cell.get_text(strip=True)]
            )

            is_thead = row.find_parent('thead') is not None
            if not has_numeric_data and (is_thead or
                has_date_patterns or
                has_years or  # Include rows with years
                meaningful_cells >= 3 or
                (i < 6 and meaningful_cells >= 2) or
                i < 4):  # Include first 4 rows as potential headers
                header_rows.append(row)

        return header_rows

    def _has_hierarchical_date_patterns(self, header_rows: List[Tag]) -> bool:
        """Check if header rows have actual hierarchical date patterns that need flattening"""
        if len(header_rows) < 2:
            return False

        # Look for date patterns in column positions across multiple rows
        # This indicates dates are split hierarchically and need combining
        date_columns_found = False

        # Check if any header row (beyond the first) contains date patterns
        # This indicates hierarchical date structure
        for i, row in enumerate(header_rows[1:], 1):  # Skip first row
            cells = row.find_all(['th', 'td'])
            for cell in cells[1:]:  # Skip first column (metrics)
                text = cell.get_text(strip=True)
                if self._has_date_pattern(text):
                    date_columns_found = True
                    break
            if date_columns_found:
                break

        return date_columns_found

    def _should_be_processed_as_type1b(self, table: Tag) -> bool:
        """Check if a table should be processed as Type 1B based on content patterns"""
        rows = table.find_all('tr')
        if len(rows) < 3:
            return False

        # Look for tables that have period-related text in the header area
        # But be more restrictive - don't trigger on obvious segment tables
        has_period_text = False
        has_obvious_segments = False

        for row in rows[:20]:  # Check first 20 rows
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                row_text = ' '.join(cell.get_text(strip=True) for cell in cells)

                # Check for period headers (more restrictive than before)
                period_indicators = ['ended', 'months', 'quarter', 'year']
                if any(indicator in row_text.lower() for indicator in period_indicators):
                    has_period_text = True

                # Check for obvious segment structures - exclude these from Type 1B
                segment_indicators = ['less than', 'greater than', 'total', 'unitedhealthcare', 'optum']
                if any(segment in row_text.lower() for segment in segment_indicators):
                    has_obvious_segments = True

        # If it has period text but NO obvious segment indicators, it's likely Type 1B
        return has_period_text and not has_obvious_segments

    def _is_metadata_cell(self, text: str) -> bool:
        """Check if cell contains metadata that shouldn't be a header"""
        if not text:
            return True

        text_lower = text.lower().strip()

        # Units and notes
        if any(phrase in text_lower for phrase in [
            'in millions', 'in thousands', 'in billions',
            'except percentages', 'per share', 'note',
            'see note', 'see accompanying'
        ]):
            return True

        # Purely numeric or symbolic
        cleaned = text.replace('.', '').replace(',', '').replace('$', '').replace('(', '').replace(')', '').replace('-', '').replace(' ', '')
        if cleaned.isdigit() or len(cleaned) < 2:
            return True

        return False

    def _generate_hash(self, headers: List[str], extracted_data: List) -> str:
        """Generate content hash for table deduplication"""
        try:
            content = f"{headers}{extracted_data}"
            return hashlib.md5(content.encode()).hexdigest()
        except Exception:
            return hashlib.md5(b"error").hexdigest()

    def _is_section_header(self, row: Tag) -> Optional[str]:
        """Check if a row is a section header"""
        cells = row.find_all(['td', 'th'])
        if not cells:
            return None

        # Check if all other cells are empty
        first_cell_text = cells[0].get_text(strip=True)
        if not first_cell_text:
            return None

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
