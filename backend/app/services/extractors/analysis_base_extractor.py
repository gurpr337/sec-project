"""
Cloned BaseExtractor for table analysis - identical logic, separate from production extraction
"""
from typing import List, Optional, Tuple
from bs4 import BeautifulSoup, Tag
import re


class AnalysisBaseExtractor:
    """Cloned base extractor for table analysis - preserves original Type 1B classification logic"""

    def classify_table_type(self, table: Tag) -> Tuple[str, str]:
        """
        Classify table type using the same logic as production BaseExtractor
        Returns: (table_type, classification_reason)
        """
        rows = table.find_all('tr')
        if len(rows) < 2:
            return 'unknown', 'Less than 2 rows'

        # Check for Type 1A - simple date header row
        type_1a_result = self._check_type1a_patterns(table)
        if type_1a_result[0]:
            return 'type_1a', type_1a_result[1]

        # Check for multi-row header patterns (Type 1B) - BEFORE Type 2 detection
        header_rows = self._identify_header_rows_for_simple(table)
        has_hierarchical = len(header_rows) > 1 and self._has_hierarchical_date_patterns(header_rows)

        # Also check for tables that should be Type 1B based on content (fallback for complex tables)
        should_be_type1b = self._should_be_processed_as_type1b(table)

        if has_hierarchical or should_be_type1b:
            reason = 'Multi-row date headers needing flattening'
            if has_hierarchical:
                reason += ' (hierarchical date patterns detected)'
            if should_be_type1b:
                reason += ' (period text without segments)'
            return 'type_1b', reason

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
            row_text = ' '.join(cell.get_text(strip=True) for cell in cells)
            segment_indicators = [
                'unitedhealthcare', 'optum', 'commercial', 'medicare', 'medicaid',
                'risk-based', 'fee-based', 'domestic', 'global', 'supplement'
            ]
            if any(indicator in row_text.lower() for indicator in segment_indicators):
                has_segment_headers = True

            if has_date_section_headers and has_segment_headers:
                return 'type_2', 'Date section headers + segment-based columns'

        # Default classification
        if has_date_section_headers:
            return 'type_1a', 'Date headers found but not Type 1A/1B/2 pattern'
        elif has_segment_headers:
            return 'type_2', 'Segment headers without clear date structure'
        else:
            return 'unknown', 'No recognizable table patterns'

    def _check_type1a_patterns(self, table: Tag) -> Tuple[bool, str]:
        """Check for Type 1A patterns - identical to production logic"""
        rows = table.find_all('tr')
        if len(rows) < 2:
            return False, 'Insufficient rows'

        # Check first few rows for date headers
        for row_idx in range(min(3, len(rows))):
            row = rows[row_idx]
            cells = row.find_all(['td', 'th'])

            if len(cells) < 2:
                continue

            # Count date headers in this row (skip first cell which is usually metric name)
            date_headers_in_row = 0
            total_data_cells = 0

            for cell_idx, cell in enumerate(cells):
                text = cell.get_text(strip=True)

                # Skip obvious non-data cells
                if not text or len(text.strip()) < 2:
                    continue

                # Skip first column (metric names)
                if cell_idx == 0:
                    continue

                total_data_cells += 1

                if self._is_date_header_text(text) or self._has_date_pattern(text):
                    if not self._is_metadata_cell(text):
                        date_headers_in_row += 1

            # If this row has mostly date headers (at least 2, and >50% of cells), it's Type 1A
            if date_headers_in_row >= 2 and date_headers_in_row >= total_data_cells * 0.5:
                return True, f'Date headers in row {row_idx + 1}: {date_headers_in_row}/{total_data_cells} cells'

        return False, 'No Type 1A patterns found'

    def _identify_header_rows_for_simple(self, table: Tag) -> List[Tag]:
        """Identify header rows for simple extraction - cloned from BaseExtractor"""
        # First priority: rows in thead
        thead = table.find('thead')
        if thead:
            header_rows = thead.find_all('tr')
            if header_rows:
                return header_rows

        # Fallback: look for rows that appear to be headers in the first part of the table
        rows = table.find_all('tr')
        header_rows = []

        for i, row in enumerate(rows[:10]):  # Check first 10 rows
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

                if text and text.strip() and len(text.strip()) > 1:
                    # Not just numbers or single characters
                    cleaned = text.replace('.', '').replace(',', '').replace('$', '').replace('(', '').replace(')', '').replace('-', '').replace(' ', '')
                    if not cleaned.isdigit() and len(cleaned) > 1:
                        meaningful_cells += 1

            # Check if this row contains date patterns (prioritize these)
            has_date_patterns = False
            for cell in cells:
                text = cell.get_text(strip=True)
                if self._has_date_pattern(text):
                    has_date_patterns = True
                    break

            # Consider it a header row if:
            # 1. It's in a thead, or
            # 2. Has date patterns (highest priority), or
            # 3. Has multiple meaningful cells, or
            # 4. First few rows (SEC tables often have complex headers)
            is_thead = row.find_parent('thead') is not None
            if (is_thead or
                has_date_patterns or
                meaningful_cells >= 3 or
                (i < 6 and meaningful_cells >= 2) or
                i < 3):  # Include first 3 rows as potential headers
                header_rows.append(row)

        return header_rows

    def _has_hierarchical_date_patterns(self, header_rows: List[Tag]) -> bool:
        """Check if header rows have actual hierarchical date patterns - cloned logic"""
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
        """Check if a table should be processed as Type 1B - cloned logic"""
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

    def _is_date_header_text(self, text: str) -> bool:
        """Check if text looks like a date header - cloned logic"""
        if not text or len(text.strip()) < 4:
            return False

        text_lower = text.lower().strip()

        # Common date header patterns
        date_patterns = [
            'december 31', 'june 30', 'march 31', 'september 30',
            'three months ended', 'six months ended', 'nine months ended', 'twelve months ended',
            'year ended', 'quarter ended'
        ]

        return any(pattern in text_lower for pattern in date_patterns)

    def _has_date_pattern(self, text: str) -> bool:
        """Check if text contains date-related patterns - cloned logic"""
        if not text:
            return False

        text_lower = text.lower()

        # Common date patterns in SEC tables
        date_indicators = [
            'ended', 'months', 'endedjune', 'endeddecember', 'endedmarch',
            'january', 'february', 'march', 'april', 'may', 'june',
            'july', 'august', 'september', 'october', 'november', 'december',
            '2025', '2024', '2023', '2022', '2021', '2020', '2019', '2018'
        ]

        return any(indicator in text_lower for indicator in date_indicators)

    def _is_metadata_cell(self, text: str) -> bool:
        """Check if cell contains metadata that shouldn't be a header - cloned logic"""
        if not text:
            return True

        text_lower = text.lower().strip()

        # Units and notes
        if any(phrase in text_lower for phrase in [
            'in millions', 'in thousands', 'in billions',
            '(in millions)', '(in thousands)', '(in billions)',
            'dollars', 'percent', 'percentage'
        ]):
            return True

        # Parenthetical expressions that are units
        if re.match(r'^\([^)]*(?:millions?|thousands?|billions?|dollars?|percent)\w*\)$', text_lower):
            return True

        # Single characters or very short
        if len(text.strip()) <= 2:
            return True

        # Notes and references
        if re.match(r'^\(\d+\)$', text.strip()) or 'note' in text_lower:
            return True

        return False

    def extract_table_title(self, table: Tag, soup) -> str:
        """Extract table title from surrounding context - cloned logic"""
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
                if not candidate.replace(',', '').replace('.', '').replace(' ', '').replace('-', '').replace('(', '').replace(')', '').isdigit():
                    valid_candidates.append(candidate)

        # Return the best candidate (prefer longer, more descriptive titles)
        if valid_candidates:
            # Sort by length (prefer longer titles) and return the first one
            valid_candidates.sort(key=len, reverse=True)
            return valid_candidates[0]

        return "Untitled Table"
