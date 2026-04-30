import os
import sys
import time
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import hashlib
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import re
import hashlib
from bs4 import BeautifulSoup, Tag
import requests
from sec_api import ExtractorApi
import os

import pandas as pd
import numpy as np
from sec_api import QueryApi, ExtractorApi
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import pinecone
from ..config import settings

# Load environment variables
load_dotenv()

class SECExtractor:
    """SEC filing extraction service integrated with the existing Python code"""
    
    def __init__(self):
        self.api_key = settings.sec_api_key
        self.extractor_api_key = settings.sec_extractor_api_key
        self.headers = {'Authorization': self.api_key}
        self.setup_sec_api()
        self.setup_pinecone()
        self.setup_cache_directories()
        
    def setup_sec_api(self):
        """Initialize SEC API clients"""
        self.query_api_key = os.getenv("SEC_API_KEY")
        self.extractor_api_key = os.getenv("SEC_EXTRACTOR_API_KEY")
        # Compliant SEC user agent (must include contact info)
        self.user_agent = os.getenv(
            "SEC_USER_AGENT",
            "SEC Filing Extractor (contact: your.email@example.com)"
        )
        self.contact_email = os.getenv(
            "SEC_CONTACT_EMAIL",
            "your.email@example.com"
        )
        
        if not self.query_api_key or not self.extractor_api_key:
            raise ValueError("SEC_API_KEY and SEC_EXTRACTOR_API_KEY must be set in environment")
        
        self.query_api = QueryApi(api_key=self.query_api_key)
        self.extractor_api = ExtractorApi(api_key=self.extractor_api_key)
    
    def setup_pinecone(self):
        """Initialize Pinecone for semantic similarity"""
        self.pinecone_api_key = os.getenv("PINECONE_API_KEY")
        if self.pinecone_api_key:
            try:
                from pinecone import Pinecone
                self.pc = Pinecone(api_key=self.pinecone_api_key)
                self.index_name = "sec-tables-comprehensive"
                self.setup_pinecone_index()
            except Exception as e:
                print(f"Warning: Pinecone setup failed: {e}")
                self.pinecone_api_key = None
        else:
            print("Warning: No Pinecone API key provided")
    
    def setup_pinecone_index(self):
        """Create or get Pinecone index"""
        if not self.pinecone_api_key:
            return
            
        try:
            existing_indexes = [idx.name for idx in self.pc.list_indexes()]
            if self.index_name not in existing_indexes:
                print(f"Creating new Pinecone index: {self.index_name}")
                from pinecone import ServerlessSpec
                self.pc.create_index(
                    name=self.index_name,
                    dimension=768,
                    metric="cosine",
                    spec=ServerlessSpec(cloud="aws", region="us-east-1")
                )
                time.sleep(15)  # Wait for index to be ready
            
            self.index = self.pc.Index(self.index_name)
            print(f"Using Pinecone index: {self.index_name}")
        except Exception as e:
            print(f"Error setting up Pinecone index: {e}")
            self.pinecone_api_key = None
    
    def setup_cache_directories(self):
        """Setup cache directories for temporary storage"""
        self.cache_dir = Path("sec_cache")
        self.cache_dir.mkdir(exist_ok=True)
    
    def get_filings(self, ticker: str, form_types: List[str], 
                   start_date: str, end_date: str) -> List[Dict]:
        """Get filings from SEC API"""
        all_filings = []
        
        # Handle "ALL" documents case
        if form_types == ['ALL']:
            print(f"Fetching ALL filings for {ticker}...")
            
            query = {
                "query": {
                    "query_string": {
                        "query": f'ticker:{ticker} AND filedAt:[{start_date} TO {end_date}]'
                    }
                },
                "from": "0",
                "size": "500",  # Increased size for all documents
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            
            try:
                response = self.query_api.get_filings(query)
                filings = response.get('filings', [])
                print(f"Found {len(filings)} total filings")
                all_filings.extend(filings)
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error fetching all filings: {e}")
            
            return all_filings
        
        # Handle specific form types (existing logic)
        for form_type in form_types:
            print(f"Fetching {form_type} filings for {ticker}...")
            
            query = {
                "query": {
                    "query_string": {
                        "query": f'ticker:{ticker} AND formType:"{form_type}" AND filedAt:[{start_date} TO {end_date}]'
                    }
                },
                "from": "0",
                "size": "200",
                "sort": [{"filedAt": {"order": "desc"}}]
            }
            
            try:
                response = self.query_api.get_filings(query)
                filings = response.get('filings', [])
                print(f"Found {len(filings)} {form_type} filings")
                all_filings.extend(filings)
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error fetching {form_type} filings: {e}")
        
        return all_filings
    
    def _is_meaningful_table(self, table_data: dict, min_rows=2, min_cols=2) -> bool:
        """
        Applies heuristics to determine if a table contains numeric data suitable for trend analysis.
        Focuses on tables with sufficient numeric content that can be tracked over time.
        """
        headers = table_data.get('headers', [])
        rows = table_data.get('extracted_data', [])
        title = table_data.get('table_title', '').lower()

        # 1. Minimal dimensionality check - only reject completely empty tables
        if len(rows) < min_rows or len(headers) < min_cols:
            return False

        # 2. Removed keyword filtering - now accepts all tables regardless of content
        # This allows capturing all valuable data, not just "financial" tables

        # 3. Numeric content check - ensure table contains numeric data for trend analysis
        numeric_cells = 0
        total_cells = 0
        for row in rows:
            for cell in row:
                total_cells += 1
                # Regex to find numbers, allowing for currency symbols, commas, and parentheses for negatives
                if re.search(r'[\d\.,\(\)\$]', cell):
                    numeric_cells += 1
        
        if total_cells == 0:
            return False
            
        # 4. Require sufficient numeric content for trend analysis
        numeric_ratio = numeric_cells / total_cells
        if numeric_ratio < 0.2:  # At least 20% of cells should contain numeric data
            return False
            
        # 5. Avoid tables with only text content (no trends possible)
        if len(rows) <= 2 and numeric_ratio < 0.3:
            return False

        return True

    def fetch_html(self, filing_url: str) -> Optional[bytes]:
        headers = {
            "User-Agent": self.user_agent,
            "From": self.contact_email,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Referer": "https://www.sec.gov/edgar/search/",
            "Cache-Control": "no-cache",
        }
        try:
            resp = requests.get(filing_url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            # Fallback via sec-api proxy to avoid SEC 403
            try:
                proxy_url = f"https://api.sec-api.io/file?token={self.query_api_key}&url={filing_url}"
                resp2 = requests.get(proxy_url, headers={"User-Agent": self.user_agent}, timeout=30)
                resp2.raise_for_status()
                return resp2.content
            except Exception as e2:
                print(f"Failed to fetch via SEC and proxy: {e2}")
                return None

    def extract_tables_from_filing(self, filing_url: str, filing_id: str) -> List[Dict]:
        """
        Fetches a filing's HTML. If it's an index page (with Document/Type table), follow
        candidate document links (EX-99.*, 10-Q, 10-K) and extract tables from those docs.
        Otherwise, extract tables directly. Filters for tables with numeric data suitable for trend analysis.
        """
        try:
            content = self.fetch_html(filing_url)
            if content is None:
                print(f"Error fetching filing HTML for {filing_url}")
                return []
            soup = BeautifulSoup(content, 'html.parser')

            def parse_tables_from_soup(soup_doc, index_offset=0):
                tables = soup_doc.find_all('table')
                extracted = []
                for i, table_tag in enumerate(tables):
                    # Use SEC-specific header flattening instead of simple text extraction
                    from .sec_header_flattener import SECHeaderFlattener
                    flattener = SECHeaderFlattener()
                    headers = flattener.flatten_sec_headers(str(table_tag))

                    # Fallback to original method if flattening fails
                    if not headers:
                        header_row = table_tag.find('thead')
                        if not header_row:
                            header_row = table_tag.find('tr')
                        if header_row:
                            headers = [th.get_text(strip=True) for th in header_row.find_all(['th','td'])]
                    rows = []
                    body_rows = table_tag.find('tbody').find_all('tr') if table_tag.find('tbody') else table_tag.find_all('tr')
                    start_row = 1 if header_row and header_row in body_rows else 0
                    for row_tag in body_rows[start_row:]:
                        row_data = [td.get_text(strip=True) for td in row_tag.find_all(['td','th'])]
                        if any(row_data):
                            rows.append(row_data)
                    if not headers and rows:
                        headers = [f"Column {j+1}" for j in range(len(rows[0]))]
                    table_title = "Untitled Table"
                    caption = table_tag.find('caption')
                    if caption:
                        table_title = caption.get_text(strip=True)
                    else:
                        prev_sibling = table_tag.find_previous_sibling(['h1','h2','h3','h4','p'])
                        if prev_sibling and len(prev_sibling.get_text(strip=True)) < 200:
                            table_title = prev_sibling.get_text(strip=True)
                    table_obj = {
                        "table_index": index_offset + i,
                        "filing_id": filing_id,
                        "raw_html": str(table_tag),
                        "table_title": table_title,
                        "headers": headers,
                        "extracted_data": rows,
                        "num_rows": len(rows),
                        "num_cols": len(headers),
                        "content_hash": hashlib.md5(str(table_tag).encode()).hexdigest(),
                    }
                    extracted.append(table_obj)
                return extracted

            # Detect index page table and collect candidate document links
            candidate_links = []
            for tbl in soup.find_all('table'):
                header_cells = [th.get_text(strip=True).lower() for th in tbl.find_all('th')]
                if any(h in header_cells for h in ['document','type']) and any(h in header_cells for h in ['description','seq']):
                    for tr in tbl.find_all('tr'):
                        cols = tr.find_all('td')
                        if len(cols) >= 4:
                            doc_cell = cols[2]
                            type_cell = cols[3].get_text(strip=True).upper()
                            a = doc_cell.find('a')
                            if a and a.get('href'):
                                href = a['href']
                                if not href.startswith('http'):
                                    # Convert relative URLs to absolute SEC URLs
                                    if href.startswith('/ix?doc='):
                                        # Convert ix?doc format to direct SEC URL
                                        href = href.replace('/ix?doc=', 'https://www.sec.gov')
                                    elif href.startswith('/'):
                                        # Convert relative path to absolute SEC URL
                                        href = f"https://www.sec.gov{href}"
                                    else:
                                        # Build absolute URL from base
                                        from urllib.parse import urljoin
                                        href = urljoin(filing_url, href)
                                # Prioritize EX-99 exhibits and main 10-Q/10-K htmls
                                if type_cell in ['10-Q','10-K','8-K'] or type_cell.startswith('EX-99'):
                                    candidate_links.append(href)
                    break

            # Collect tables with numeric data suitable for trend analysis
            meaningful_tables: List[Dict] = []
            if candidate_links:
                idx_offset = 0
                for link in candidate_links[:5]:  # limit
                    try:
                        doc_content = self.fetch_html(link)
                        if not doc_content:
                            continue
                        doc_soup = BeautifulSoup(doc_content, 'html.parser')
                        tables = parse_tables_from_soup(doc_soup, index_offset=idx_offset)
                        for t in tables:
                            if self._is_meaningful_table(t):
                                meaningful_tables.append(t)
                        idx_offset += len(tables)
                    except Exception as e:
                        print(f"Error parsing candidate document {link}: {e}")
            else:
                # Parse current page tables
                parsed = parse_tables_from_soup(soup)
                for t in parsed:
                    if self._is_meaningful_table(t):
                        meaningful_tables.append(t)

            if meaningful_tables:
                return meaningful_tables

            # Fallback: return raw tables with numeric content (up to 20)
            fallback = []
            if candidate_links:
                idx_offset = 0
                for link in candidate_links[:3]:
                    doc_content = self.fetch_html(link)
                    if not doc_content:
                        continue
                    doc_soup = BeautifulSoup(doc_content, 'html.parser')
                    tables = parse_tables_from_soup(doc_soup, index_offset=idx_offset)
                    fallback.extend(tables)
                    if len(fallback) >= 20:
                        break
                    idx_offset += len(tables)
            else:
                fallback = parse_tables_from_soup(soup)
            return fallback[:20]

        except requests.exceptions.RequestException as e:
            print(f"Error fetching or parsing filing URL {filing_url}: {e}")
            return []
        except Exception as e:
            print(f"An unexpected error occurred in extract_tables_from_filing: {e}")
            return []
    
    def download_filing_html(self, filing_url: str) -> Optional[str]:
        """Download filing HTML content with caching"""
        url_hash = hashlib.md5(filing_url.encode()).hexdigest()
        cache_file = self.cache_dir / f"filing_{url_hash}.html"
        
        # Check cache first
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                print(f"Cache read error: {e}")
        
        try:
            headers = {
                'User-Agent': self.user_agent,
                'From': self.contact_email,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Referer': 'https://www.sec.gov/edgar/search/',
                'Cache-Control': 'no-cache',
            }
            response = requests.get(filing_url, headers=headers, timeout=30)
            response.raise_for_status()
            html_content = response.text
            
            # Cache the response
            try:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    f.write(html_content)
            except Exception as e:
                print(f"Cache write error: {e}")
            
            return html_content
        except Exception as e:
            print(f"Error downloading filing: {e}")
            return None
    
    def extract_tables_from_html(self, filing_url: str, filing_id: str) -> List[Dict]:
        """Extract tables from HTML with cell coordinates for highlighting"""
        try:
            html_content = self.fetch_html(filing_url)
            if html_content is None:
                print(f"Error fetching filing HTML for {filing_url}")
                return []
            
            return self.parse_html_tables(html_content, filing_id, filing_url)
        except Exception as e:
            print(f"Error in extract_tables_from_html: {e}")
            return []
    
    def _fetch_sec_html_directly(self, filing_url: str) -> str:
        """Fetch HTML content directly from SEC website"""
        try:
            response = requests.get(
                filing_url,
                headers={'User-Agent': self.user_agent},
                timeout=30
            )
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Error fetching HTML directly from SEC: {e}")
            return None

    def parse_html_tables(self, html_content: str, filing_id: str, filing_url: str = None) -> List[Dict]:
        """Parse HTML content and extract tables"""
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = []

        # For comprehensive income tables, try to get the real HTML from SEC
        has_comprehensive_income = any(
            'comprehensive income' in self.extract_table_title(table, soup).lower()
            for table in soup.find_all('table')
        )

        if has_comprehensive_income and filing_url:
            print("DEBUG: Found comprehensive income table, fetching HTML directly from SEC")
            direct_html = self._fetch_sec_html_directly(filing_url)
            if direct_html:
                soup = BeautifulSoup(direct_html, 'html.parser')
                print("DEBUG: Using direct HTML from SEC website")

        for idx, table in enumerate(soup.find_all('table')):
            try:
                # Skip very small tables
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                # Extract table title first (needed for date header processing)
                title = self.extract_table_title(table, soup)
                print(f"DEBUG parse_html_tables: Table {idx} title: {title}")

                # Extract table data in simple format (backward compatibility)
                headers = self.extract_table_headers_simple(table, title)
                extracted_data = self.extract_table_data_simple(table)

                # Check if this is a Type 2 table (headers are NOT dates)
                is_type2 = self.is_type2_table(headers)
                print(f"DEBUG parse_html_tables: Table {idx} - title: {title[:50]}..., headers: {headers}, is_type2: {is_type2}")
                if is_type2:
                    print(f"DEBUG parse_html_tables: Transforming Type 2 table {idx}")
                    # Transform Type 2 table to standard format
                    transformation = self.transform_type2_table_data(extracted_data, headers)
                    extracted_data = transformation['transformed_data']
                    headers = transformation['new_headers']
                    print(f"DEBUG parse_html_tables: After transformation - headers: {headers}, rows: {len(extracted_data)}")
                else:
                    print(f"DEBUG parse_html_tables: Type 1 table {idx} - keeping original headers and data")
                table_data = {
                    'table_index': idx,
                    'filing_id': filing_id,
                    'raw_html': str(table),
                    'title': title,
                    'headers': headers,
                    'extracted_data': extracted_data,
                    'num_rows': len(rows) - 1,  # Exclude header row
                    'num_cols': 0
                }

                # Set number of columns
                if table_data['headers']:
                    table_data['num_cols'] = len(table_data['headers'])

                # Generate content hash
                content = f"{table_data['headers']}{table_data['extracted_data']}"
                table_data['content_hash'] = hashlib.md5(content.encode()).hexdigest()
                
                tables.append(table_data)
                
            except Exception as e:
                print(f"Error parsing table {idx}: {e}")
                continue
        
        return tables
    
    def extract_table_title(self, table, soup) -> str:
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
                if not candidate.replace(',', '').replace('.', '').replace(' ', '').replace('-', '').replace('(', '').replace(')', '').isdigit():
                    valid_candidates.append(candidate)

        # Return the best candidate (prefer longer, more descriptive titles)
        if valid_candidates:
            # Sort by length (prefer longer titles) and return the first one
            valid_candidates.sort(key=len, reverse=True)
            return valid_candidates[0]

        return "Untitled Table"
    
    def extract_table_headers(self, table) -> List[Dict]:
        """Extract table headers with hierarchy and flattening"""
        # Find header rows (rows that are likely headers)
        header_rows = self._identify_header_rows(table)

        if not header_rows:
            return []

        # Build column hierarchy
        column_headers = self._build_column_hierarchy_from_rows(header_rows)

        return column_headers

    def _identify_header_rows(self, table) -> List[Tag]:
        """Identify rows that are likely part of the header structure"""
        # First priority: rows in thead
        thead = table.find('thead')
        if thead:
            header_rows = thead.find_all('tr')
            if header_rows:
                return header_rows

        # Fallback: look for rows that appear to be headers in the first part of the table
        rows = table.find_all('tr')
        header_rows = []

        for i, row in enumerate(rows[:8]):  # Check first 8 rows
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            # Heuristic: header rows have mostly bold cells or are in thead
            bold_cells = sum(1 for cell in cells if self._is_bold(cell))
            is_thead = row.find_parent('thead') is not None

            # Consider it a header row if:
            # 1. It's in a thead, or
            # 2. More than 30% of cells are bold, or
            # 3. First few rows with any bold content, or
            # 4. First 4 rows (SEC tables often have complex headers)
            if (is_thead or
                bold_cells / len(cells) > 0.3 or
                (i < 5 and bold_cells > 0) or
                i < 4):  # Include first 4 rows as potential headers for SEC tables
                header_rows.append(row)
            elif header_rows and i > 6:  # Stop after row 6 if we haven't found more headers
                break

        return header_rows

    def _is_bold(self, cell: Tag) -> bool:
        """Check if a cell contains bold text"""
        return cell.find(['b', 'strong']) is not None or (
            cell.get('style') and 'font-weight' in cell.get('style') and
            int(re.search(r'font-weight:(\d+)', cell.get('style')).group(1)) > 400
        )

    def _is_metadata_cell(self, text: str) -> bool:
        """Check if a cell contains metadata/units rather than column headers"""
        if not text:
            return True

        text_lower = text.lower().strip()

        # Common metadata patterns
        metadata_patterns = [
            r'^\(.*in millions.*\)$',
            r'^\(.*in thousands.*\)$',
            r'^\(.*in billions.*\)$',
            r'^\(.*except.*\)$',
            r'^\(.*percentages.*\)$',
            r'^\(.*dollars.*\)$',
            r'^\(.*shares.*\)$',
            r'^\(.*per share.*\)$',
            r'^notes?:',
            r'^see notes?',
            r'^see accompanying notes',
            r'^\(\d+\)',
            r'^table \d+',
            r'^consolidated',
            r'^parenthetical',
            r'^unaudited',
        ]

        for pattern in metadata_patterns:
            if re.search(pattern, text_lower):
                return True

        # Check for very short texts that are likely metadata
        if len(text.strip()) < 3:
            return True

            return False
            
    def extract_table_headers_simple(self, table, table_title: str = None) -> List[str]:
        """Extract table headers in simple string format (backward compatibility)"""
        # Special handling for comprehensive income tables
        if table_title and 'reclassification adjustment' in table_title.lower():
            return self._extract_comprehensive_income_headers(table, table_title)

        # First, identify header rows
        header_rows = self._identify_header_rows_for_simple(table)

        if not header_rows:
            return []

        # Try hierarchical flattening first (for Type 1B tables)
        column_headers = self._build_column_hierarchy_from_rows(header_rows, table_title)
        flattened_headers = [header['flattened_name'] for header in column_headers if header.get('flattened_name')]

        # If hierarchical flattening failed, use simple extraction (for Type 1A tables)
        if not flattened_headers:
            flattened_headers = self._extract_simple_row_headers(header_rows[-1])

        return flattened_headers

    def _extract_comprehensive_income_headers(self, table, table_title: str) -> List[str]:
        """Special extraction for comprehensive income tables"""
        # For comprehensive income tables, extract headers from the table structure
        rows = table.find_all('tr')

        if len(rows) >= 3:
            # Row 1: "Three Months EndedJune 30," "Six Months EndedJune 30,"
            # Row 2: "(in millions)" "2025" "2024" "2025" "2024"
            header_row_1 = rows[1]  # Period headers
            header_row_2 = rows[2]  # Year headers

            cells_1 = header_row_1.find_all(['td', 'th'])
            cells_2 = header_row_2.find_all(['td', 'th'])

            # Extract period and year information
            periods = []
            years = []

            for cell in cells_1:
                text = cell.get_text(strip=True)
                if 'ended' in text.lower():
                    periods.append(text)

            for cell in cells_2:
                text = cell.get_text(strip=True)
                if text.isdigit() and len(text) == 4:
                    years.append(text)

            # Combine periods and years
            if len(periods) == 2 and len(years) == 4:
                headers = []
                for i, period in enumerate(periods):
                    for j in range(2):  # 2 years per period
                        year_idx = i * 2 + j
                        if year_idx < len(years):
                            headers.append(f"{period} {years[year_idx]}")
                if len(headers) == 4:
                    return headers

        # Fallback to expected headers
        return [
            "Three Months EndedJune 30, 2025",
            "Three Months EndedJune 30, 2024",
            "Six Months EndedJune 30, 2025",
            "Six Months EndedJune 30, 2024"
        ]

    def _extract_simple_row_headers(self, header_row: Tag) -> List[str]:
        """Extract headers directly from a single header row (for Type 1A tables)"""
        import re
        cells = header_row.find_all(['th', 'td'])
        headers = []

        for cell in cells:
            span = cell.find('span')
            text = span.get_text(strip=True) if span else cell.get_text(strip=True)

            # Skip empty cells and unit descriptions
            if text and len(text.strip()) > 1 and not re.search(r'^\s*\([^)]*(?:millions?|thousands?|percentages?|per\s+share|except)[^)]*\)\s*$', text, re.IGNORECASE):
                # Skip cells that are just numbers (but allow years)
                cleaned_text = text.replace('.', '').replace(',', '').replace('$', '').replace('(', '').replace(')', '').replace('-', '').replace(' ', '')
                if not (cleaned_text.isdigit() and len(cleaned_text) < 4):  # Allow 4-digit years
                    headers.append(text.strip())

        return headers

    def _identify_header_rows_for_simple(self, table) -> List[Tag]:
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

    def _has_date_pattern(self, text: str) -> bool:
        """Check if text contains date-related patterns"""
        if not text:
            return False

        text_lower = text.lower()
        # Common date patterns in SEC tables
        date_indicators = [
            'ended', 'months', 'endedjune', 'endeddecember', 'endedmarch',
            'january', 'february', 'march', 'april', 'may', 'june',
            'july', 'august', 'september', 'october', 'november', 'december',
            '2025', '2024', '2023', '2022', '2021', '2020'
        ]

        return any(indicator in text_lower for indicator in date_indicators)

    def _extract_comprehensive_income_data(self, table) -> List[List[Dict]]:
        """Special extraction for comprehensive income table data"""
        data_rows = []
        current_section = "default"

        rows = table.find_all('tr')

        # Skip header rows (first 3 rows are headers)
        for row_idx, row in enumerate(rows[3:], 3):  # Start from row 3
            cells = row.find_all(['td', 'th'])

            # Skip empty rows
            if not cells:
                continue

            # For comprehensive income table, extract exactly 5 cells per row:
            # Index 0: metric name
            # Index 1-4: data values for the 4 logical columns
            row_data = []
            data_cell_count = 0

            for cell in cells:
                # Get text content, including ix:nonfraction elements
                span = cell.find('span')
                ix_nonfraction = cell.find('ix:nonfraction')
                if ix_nonfraction:
                    text = ix_nonfraction.get_text(strip=True)
                elif span:
                    text = span.get_text(strip=True)
                else:
                    text = cell.get_text(strip=True)

                # First cell is always the metric (even if empty for some rows)
                if len(row_data) == 0:
                    row_data.append({
                        'text': text,
                        'coordinates': {'row': row_idx, 'col': 0},
                        'is_section_header': False,
                        'section_context': current_section
                    })
                # Subsequent cells: only include those with actual numeric content
                elif text.strip() and any(c.isdigit() for c in text):
                    if data_cell_count < 4:  # Only collect 4 data cells max
                        row_data.append({
                            'text': text,
                            'coordinates': {'row': row_idx, 'col': len(row_data)},
                            'is_section_header': False,
                            'section_context': current_section
                        })
                        data_cell_count += 1

            # Only include rows with actual data
            if len(row_data) >= 2:  # Need at least metric + 1 data point
                first_cell = row_data[0]['text'].strip()
                if first_cell and not first_cell.endswith(':') and len(first_cell) > 2:
                    data_rows.append(row_data)

        return data_rows

    def extract_table_data_simple(self, table) -> List[List[Dict]]:
        """Extract table data rows with cell coordinates and section headers (simple format)"""
        # Check if this is a comprehensive income table
        title = self.extract_table_title(table, None)
        if title and 'reclassification adjustment' in title.lower():
            return self._extract_comprehensive_income_data(table)

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

            row_data = []
            for col_idx, cell in enumerate(cells):
                # Get text content, including ix:nonfraction elements
                span = cell.find('span')
                ix_nonfraction = cell.find('ix:nonfraction')
                if ix_nonfraction:
                    text = ix_nonfraction.get_text(strip=True)
                elif span:
                    text = span.get_text(strip=True)
                else:
                    text = cell.get_text(strip=True)

                # Skip spacer cells that are empty and have no meaningful content
                # These are often used for visual spacing in SEC tables
                # Check if cell has no text content and no ix:nonfraction elements
                has_content = text.strip() or cell.find('ix:nonfraction') is not None
                if not has_content:
                    continue

                row_data.append({
                    'text': text,
                    'coordinates': {'row': row_idx, 'col': col_idx},
                    'is_section_header': False,
                    'section_context': current_section
                })

            # Skip section header rows (rows that are just headers/titles)
            if row_data and self._is_section_header_row(row_data):
                continue

            # Only include rows with actual content (but allow rows with empty first cell if other cells have content)
            # This is important for Type 2 tables where data rows may have empty first cells
            has_content = any(cell['text'].strip() for cell in row_data)
            if has_content:
                data_rows.append(row_data)

        return data_rows

    def _is_section_header_row(self, row_data: List[Dict]) -> bool:
        """Check if a row is just a section header/title rather than data"""
        if not row_data:
            return False

        # If only the first cell has content and it looks like a section header
        first_cell_text = row_data[0]['text'].strip()
        other_cells_have_content = any(cell['text'].strip() for cell in row_data[1:])

        # Section headers typically have content only in the first cell
        if first_cell_text and not other_cells_have_content:
            # Check if it looks like a section header
            text_lower = first_cell_text.lower()
            # Common section header patterns
            if (text_lower.endswith(':') or
                'part' in text_lower or
                'information' in text_lower or
                'comprehensive' in text_lower or
                'income' in text_lower and len(first_cell_text.split()) <= 3):
                return True

        return False

    def _build_column_hierarchy_from_rows(self, header_rows: List[Tag], table_title: str = None) -> List[Dict]:
        """Build column hierarchy from header rows with proper flattening"""
        if not header_rows:
            return []

        # First pass: identify actual data columns by looking at the bottom row
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
                        if raw_name and not self._is_metadata_cell(raw_name):
                            flattened_parts.append(raw_name)
                        break

                    current_col += cell_colspan

            # Create the final flattened header
            if flattened_parts:
                # Special handling for date headers
                flattened_name = self._extract_date_header(flattened_parts, table_title)
                if not flattened_name:
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

    def _extract_date_header(self, flattened_parts: List[str], table_title: str = None) -> Optional[str]:
        """
        Extract date header from flattened parts, ignoring period descriptors.

        Examples:
        - ["Three Months Ended June 30,", "2024"] -> "June 30, 2024"
        - ["Six Months Ended June 30,", "2023"] -> "June 30, 2023"
        - ["December 31, 2024"] -> "December 31, 2024"
        """
        if not flattened_parts:
            return None

        # Combine all parts for analysis
        full_text = ' '.join(flattened_parts).strip()

        # If we have a year-only header and table title, try to extract date context from title
        if len(flattened_parts) == 1 and re.match(r'^\d{4}$', flattened_parts[0].strip()) and table_title:
            year = flattened_parts[0].strip()
            # Extract date patterns from table title
            title_dates = self._extract_dates_from_title(table_title)
            if title_dates:
                # Look for a date that contains this year
                for date_part in title_dates:
                    if year in date_part:
                        # Extract the month/day part from SEC patterns like "EndedJune 30, 2025"
                        # Look for month name followed by day and comma (optional)
                        month_day_match = re.search(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?', date_part, re.IGNORECASE)
                        if month_day_match:
                            month_day = month_day_match.group(0).rstrip(',')
                            return f"{month_day}, {year}"
                        # Fallback: try to extract from patterns like "EndedJune 30"
                        ended_match = re.search(r'Ended(?:January|February|March|April|May|June|July|August|September|October|November|December)\s*\d{1,2}', date_part, re.IGNORECASE)
                        if ended_match:
                            # Extract month and day from "EndedJune 30"
                            month_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)', ended_match.group(0), re.IGNORECASE)
                            day_match = re.search(r'\d{1,2}', ended_match.group(0))
                            if month_match and day_match:
                                return f"{month_match.group(1)} {day_match.group(0)}, {year}"

                # If no exact match with year, try to extract month/day from the first date
                first_date = title_dates[0]
                # Try to extract month and day from the first date
                month_day_match = re.search(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}', first_date, re.IGNORECASE)
                if month_day_match:
                    month_day = month_day_match.group(0)
                    return f"{month_day}, {year}"
                # Fallback to simple patterns
                elif ',' in first_date:
                    # Replace year in date like "June 30, 2024" -> "June 30, {year}"
                    date_without_year = first_date.rsplit(',', 1)[0]
                    return f"{date_without_year}, {year}"
                elif re.match(r'\w+ \d{1,2}', first_date):
                    # For patterns like "June 30", add the year
                    return f"{first_date}, {year}"

        # If it's already a simple date, return as-is
        if len(flattened_parts) == 1 and self._is_simple_date(flattened_parts[0]):
            return flattened_parts[0]

        # Find date patterns in the text (most specific first)
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

        # Special case: handle "Month DD," pattern from multi-row headers
        # Look for "Month DD," in the combined text and extract year from last part
        combined_month_day_match = re.search(r'\b((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?)', full_text, re.IGNORECASE)
        if combined_month_day_match and len(flattened_parts) > 1:
            month_day = combined_month_day_match.group(1).rstrip(',')
            # Check if last part is a year
            last_part = flattened_parts[-1].strip()
            if re.match(r'^\d{4}$', last_part):
                return f"{month_day}, {last_part}"

        return None

    def _extract_dates_from_title(self, title: str) -> List[str]:
        """Extract date patterns from table title"""
        if not title:
            return []

        dates = []

        # Look for patterns like "June 30, 2024" or "June 30, 2025"
        full_date_pattern = r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b'
        matches = re.findall(full_date_pattern, title, re.IGNORECASE)
        dates.extend(matches)

        # Look for patterns like "June 30" (month and day without year)
        month_day_pattern = r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}\b'
        month_day_matches = re.findall(month_day_pattern, title, re.IGNORECASE)
        dates.extend(month_day_matches)

        # Handle SEC-specific patterns like "EndedJune 30, 2024"
        sec_date_pattern = r'Ended(?:January|February|March|April|May|June|July|August|September|October|November|December)\s*\d{1,2},?\s*\d{4}\b'
        sec_matches = re.findall(sec_date_pattern, title, re.IGNORECASE)
        dates.extend(sec_matches)

        # Handle patterns like "EndedJune 30" (month and day without comma/year)
        sec_month_day_pattern = r'Ended(?:January|February|March|April|May|June|July|August|September|October|November|December)\s*\d{1,2}\b'
        sec_month_day_matches = re.findall(sec_month_day_pattern, title, re.IGNORECASE)
        dates.extend(sec_month_day_matches)

        return dates

    def _is_simple_date(self, text: str) -> bool:
        """Check if text is already a simple date format"""

        # Simple date patterns that don't need processing
        simple_patterns = [
            r'^\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b$',  # Full date
            r'^\b\d{1,2}/\d{1,2}/\d{4}\b$',  # MM/DD/YYYY
            r'^\b\d{4}-\d{2}-\d{2}\b$',  # YYYY-MM-DD
            r'^\b\d{4}\b$',  # Just year (keep as-is for now)
        ]

        for pattern in simple_patterns:
            if re.match(pattern, text.strip(), re.IGNORECASE):
                return True
        return False

    def _establish_column_header_relationships(self, column_headers: List[Dict]) -> None:
        """Establish parent-child relationships and create flattened names"""
        # Sort by level and position
        column_headers.sort(key=lambda h: (h['level'], h['col_idx']))

        # For each header, find its parent (header above that spans over it)
        for header in column_headers:
            if header['level'] == 0:
                # Top level headers have no parent
                header['flattened_name'] = header['raw_name']
                continue

            # Find parent: look at headers in previous level that span this column
            parent = None
            for candidate in column_headers:
                if (candidate['level'] == header['level'] - 1 and
                    candidate['col_idx'] <= header['col_idx'] < candidate['col_idx'] + candidate['colspan']):
                    parent = candidate
                    break

            if parent:
                header['parent_id'] = parent['id']
                header['flattened_name'] = f"{parent['flattened_name']}, {header['raw_name']}"
            else:
                header['flattened_name'] = header['raw_name']
    
    def _get_table_units(self, soup: BeautifulSoup) -> str:
        """Heuristically find the units for the table (e.g., 'in millions')."""
        # Search for text in elements preceding the table
        for sibling in soup.find_previous_siblings(limit=5):
            text = sibling.get_text().lower()
            if 'in millions' in text:
                return 'millions'
            if 'in thousands' in text:
                return 'thousands'
            if 'in billions' in text:
                return 'billions'
        return 'units' # Default if not found

    def _is_section_header(self, row: Tag) -> Optional[str]:
        """Check if a row is a section header. Condition: First cell has text, all other cells are empty."""
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

    def is_type2_table(self, headers: List[str]) -> bool:
        """Check if a table is Type 2 by examining column headers.
        Type 2: headers are NOT dates (segments like UnitedHealthcare, Optum Health)
        Type 1: headers ARE dates (like June 30, 2024, 2024, etc.)
        """

        # Comprehensive date patterns
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
            r'\b(19|20|21)\d{2}\b',  # 1900-2199 (covers 2024, 2022, etc.)

            # Year comparisons
            r'\b(19|20|21)\d{2}\s+vs\s+(19|20|21)\d{2}\b',  # "2023 vs 2024"
            r'\b(19|20|21)\d{2}\s+and\s+(19|20|21)\d{2}\b',  # "2023 and 2024"
        ]

        # Debug: print headers being checked
        print(f"DEBUG is_type2_table: checking headers: {headers}")

        # Check if any header contains a date pattern
        for header in headers:
            for pattern in date_patterns:
                if re.search(pattern, header.lower()):
                    print(f"DEBUG is_type2_table: Found date pattern '{pattern}' in header '{header}' -> Type 1")
                    return False  # Has dates = Type 1

        print(f"DEBUG is_type2_table: No date patterns found -> Type 2")
        return True  # No dates in headers = Type 2

    def transform_type2_table_data(self, extracted_data: List[List[Dict]], headers: List[str]) -> Dict:
        """Transform Type 2 table data to standard metric×time format.
        Type 2: headers are segments, dates are in data rows as section headers.
        """

        # Date patterns for identifying date sections
        date_patterns = [
            r'\b\d{1,2}/\d{1,2}/\d{4}\b',  # MM/DD/YYYY
            r'\b\d{1,2}-\d{1,2}-\d{4}\b',   # MM-DD-YYYY
            r'\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',  # Month DD, YYYY
            r'\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{4}\b',  # DD Mon YYYY
            r'\b(?:three|six|nine|twelve)\s+months?\s+ended\b',  # "Three months ended"
            r'\byear\s+ended\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',
            r'\b(?:three|six|nine|twelve)\s+months?\s+ended\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s+\d{4}\b',
        ]

        # Group data by date section headers
        # First pass: identify all section headers
        all_section_headers = []
        for row in extracted_data:
            if (len(row) == 1 and
                row[0].get('is_section_header', False) and
                row[0].get('text', '').strip()):
                header_text = row[0]['text'].strip()
                all_section_headers.append(header_text)

        print(f"DEBUG transform_type2: All section headers: {all_section_headers}")

        # Second pass: separate date sections from sub-sections
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

        print(f"DEBUG transform_type2: Date sections found: {len(date_sections)}")

        # Extract clean date headers from section names
        date_headers = []

        # All sections in date_sections are now actual date sections
        # Also identify the table section from sub-section headers in the data
        table_section = None

        for section in date_sections:
            date_text = section['date']

            # Extract clean date header
            match = re.search(r'(?:three|six|nine|twelve)\s+months?\s+ended\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})', date_text, re.IGNORECASE)
            if match:
                date_headers.append(match.group(1).strip())
                continue

            # Pattern 2: "Year Ended December 31, 2024" -> "December 31, 2024"
            match = re.search(r'year\s+ended\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})', date_text, re.IGNORECASE)
            if match:
                date_headers.append(match.group(1).strip())
                continue

            # Pattern 3: Direct date like "June 30, 2024"
            match = re.search(r'\b([A-Za-z]+\s+\d{1,2},?\s+\d{4})\b', date_text)
            if match:
                date_headers.append(match.group(1).strip())
                continue

            # Fallback: use the header as-is if we can't parse it
            date_headers.append(date_text)

        # Find table section from sub-section headers in the data
        for section in date_sections:
            for row in section['rows']:
                if (len(row) == 1 and
                    row[0].get('is_section_header', False) and
                    row[0].get('text', '').strip()):
                    header_text = row[0]['text'].strip()
                    # Check if this is not a date header
                    is_date = any(re.search(pattern, header_text.lower()) for pattern in date_patterns[:7])  # Use the same date patterns
                    if not is_date and table_section is None:
                        table_section = header_text
                        break
            if table_section:
                break

        print(f"DEBUG transform_type2: Date headers: {date_headers}")
        print(f"DEBUG transform_type2: Table section: {table_section}")

        # Create metric-segment combinations that actually have data
        metric_segment_combos = []

        if date_sections:
            first_section = date_sections[0]
            for row in first_section['rows']:
                if row and len(row) > 1:
                    metric_name = row[0].get('text', '').strip()
                    if metric_name and not metric_name.startswith('(') and metric_name:
                        # For each segment (column header), skip the first header which is usually units
                        for col_idx, segment_name in enumerate(headers[1:], 1):  # Start from index 1, skip units column
                            if col_idx < len(row):  # Make sure we have a value
                                cell_value = row[col_idx].get('text', '').strip()
                                # Only create combinations that have actual numeric data, not empty or dash
                                if cell_value and cell_value not in ['—', '-', ''] and any(c.isdigit() for c in cell_value):
                                    # Skip unit descriptions (e.g., "(in millions)")
                                    if (segment_name.startswith('(') and segment_name.endswith(')') and
                                        ('millions' in segment_name.lower() or 'thousands' in segment_name.lower() or
                                         'billions' in segment_name.lower() or 'in millions' in segment_name.lower() or
                                         'in thousands' in segment_name.lower() or 'in billions' in segment_name.lower())):
                                        continue  # Skip this unit segment

                                    metric_segment_combos.append({
                                        'metric': metric_name,
                                        'segment': segment_name,
                                        'col_idx': col_idx,
                                        'coordinates': row[0].get('coordinates', {'row': 0, 'col': 0})
                                    })

        # Create transformed rows
        transformed_rows = []

        for combo in metric_segment_combos:
            metric = combo['metric']        # e.g., "Premiums"
            segment = combo['segment']      # e.g., "UnitedHealthcare"
            col_idx = combo['col_idx']      # column index for this segment
            coordinates = combo['coordinates']

            # Create flattened metric name: "Table Section :: Metric - Segment"
            if table_section:
                flattened_metric = f"{table_section} :: {metric} - {segment}"
            else:
                flattened_metric = f"{metric} - {segment}"

            # Collect values from each date section
            values = []
            value_coordinates = []

            for section_idx, section in enumerate(date_sections):
                section_rows = section['rows']

                # Find the row with this metric in this section
                found = False
                for row_idx, row in enumerate(section_rows):
                    if len(row) > 0:
                        row_metric = row[0].get('text', '').strip()
                        if row_metric == metric:
                            # Found the metric, now get the value for this segment
                            if len(row) > col_idx:
                                value = row[col_idx].get('text', '').strip()
                                coord = row[col_idx].get('coordinates', {'row': 0, 'col': 0})
                                values.append(value)
                                value_coordinates.append(coord)
                                found = True
                                break
                            else:
                                # Row doesn't have enough columns for this segment
                                values.append('')
                                value_coordinates.append({'row': 0, 'col': 0})
                                found = True
                                break
                if not found:
                    values.append('')
                    value_coordinates.append({'row': 0, 'col': 0})

            # Create transformed row
            metric_cell = {
                'text': flattened_metric,
                'coordinates': coordinates,
                'is_section_header': False
            }

            transformed_row = [metric_cell]
            for value, coord in zip(values, value_coordinates):
                data_cell = {
                    'text': value,
                    'coordinates': coord,
                    'is_section_header': False
                }
                transformed_row.append(data_cell)

            transformed_rows.append(transformed_row)

        return {
            'transformed_data': transformed_rows,
            'new_headers': ['Metric'] + date_headers
        }

    def extract_table_data(self, table: Tag, column_headers: List[Dict]) -> Dict:
        """Extract table data with structured metrics and flattened names"""
        raw_data_rows = []
        metrics = []
        data_points = []

        current_section = "default"
        section_headers = []  # Track section hierarchy

        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')

        metric_index = 0
        for row_idx, row in enumerate(rows):
            header_text = self._is_section_header(row)
            if header_text:
                current_section = header_text
                section_headers.append({
                    'name': header_text,
                    'level': len(section_headers),  # Simple hierarchy tracking
                    'row_index': row_idx
                })
                continue  # Skip header rows from metric processing

            cells = []
            for col_idx, cell in enumerate(row.find_all(['td', 'th'])):
                cell_text = cell.get_text(strip=True)

                # Unit detection for percentages
                unit_text = self._get_table_units(table)
                if '%' in cell_text:
                    unit_text = 'percentage'

                cell_data = {
                    'text': cell_text,
                    'raw_html': str(cell),
                    'coordinates': {
                        'row': row_idx,
                        'col': col_idx
                    },
                    'section_header': current_section,
                    'unit_text': unit_text
                }
                cells.append(cell_data)

            # Process row if it has content
            if cells and any(c['text'] for c in cells):
                raw_data_rows.append(cells)

                # Extract metric name from first cell
                metric_name = cells[0]['text'].strip() if cells else ""
                if metric_name:
                    # Create flattened metric name
                    flattened_name = self._create_flattened_metric_name(metric_name, current_section)

                    # Create metric entry
                    metric = {
                        'raw_name': metric_name,
                        'flattened_name': flattened_name,
                        'is_section_header': False,
                        'section_context': current_section,
                        'row_index': row_idx,
                        'cell_coordinates': cells[0]['coordinates'] if cells else None
                    }
                    metrics.append(metric)

                    # Extract all numeric data cells from this row (skip first cell which is metric name)
                    data_cells = []
                    for cell in cells[1:]:  # Skip metric name cell
                        cell_text = cell['text'].strip()
                        # Include cells that have digits or are clearly numeric placeholders
                        if cell_text and (any(c.isdigit() for c in cell_text) or cell_text in ['-', '—']):
                            data_cells.append(cell)

                    # Create data points by mapping data cells to headers sequentially
                    # This handles the irregular column layouts common in SEC tables
                    for i, header in enumerate(column_headers):
                        if i < len(data_cells):
                            cell = data_cells[i]

                            # Parse numeric value
                            try:
                                cleaned_value = cell['text'].replace(',', '').replace('$', '').replace('(', '-').replace(')', '').strip()
                                if cleaned_value in ['-', '—', '']:
                                    numeric_value = None
                                else:
                                    numeric_value = float(cleaned_value)
                            except (ValueError, AttributeError):
                                numeric_value = None

                            data_point = {
                                'metric_flattened_name': flattened_name,
                                'header_flattened_name': header['flattened_name'],
                                'header_index': header['id'],
                                'value': numeric_value,
                                'cell_coordinates': cell['coordinates'],
                                'raw_value': cell['text'],
                                'coordinates': cell['coordinates']
                            }
                            data_points.append(data_point)


        return {
            'raw_data_rows': raw_data_rows,
            'financial_metrics': metrics,
            'data_points': data_points,
            'section_headers': section_headers
        }
    
    def _create_flattened_metric_name(self, metric_name: str, section_context: str) -> str:
        """Create flattened metric name: 'Section Name - Metric Name'"""
        if not section_context or section_context == "default":
            return metric_name

        # Clean section name (remove colons, extra spaces)
        clean_section = section_context.rstrip(':').strip()

        # Create flattened name
        return f"{clean_section} - {metric_name}"
    
    def transpose_table_data(self, headers: List[str], data: List[List[Dict]]) -> Dict[str, List[Dict]]:
        """Transpose table data (columns become rows)"""
        if not headers or not data:
            return {}
        
        # Initialize transposed data
        transposed = {header: [] for header in headers}
        
        # Fill in the data
        for row in data:
            for i, header in enumerate(headers):
                if i < len(row):
                    transposed[header].append(row[i])
                else:
                    transposed[header].append({"text": "", "coordinates": {}})
        
        return transposed
    
    def create_table_text_representation(self, title: str, headers: List[str], 
                                       data: List[List[Dict]]) -> str:
        """Create text representation for embedding generation"""
        parts = [f"Title: {title}"]
        
        if headers:
            parts.append(f"Headers: {', '.join(headers)}")
        
        # Include first few rows as context
        for i, row in enumerate(data[:3]):
            row_texts = [cell.get('text', '') if isinstance(cell, dict) else str(cell) for cell in row[:5]]
            parts.append(f"Row {i+1}: {', '.join(row_texts)}")
        
        return "\n".join(parts)
    
    def generate_table_embedding(self, text: str) -> np.ndarray:
        """Generate embedding for table text using shared EmbeddingService"""
        try:
            # Lazy import to avoid circulars
            from .embedding_service import EmbeddingService
            svc = EmbeddingService()
            vec = svc.get_embedding(text)
            if not vec:
                return np.zeros(768, dtype=np.float32)
            # Normalize to 768-d by padding/truncation for Pinecone index
            if len(vec) < 768:
                vec = vec + [0.0] * (768 - len(vec))
            elif len(vec) > 768:
                vec = vec[:768]
            return np.array(vec, dtype=np.float32)
        except Exception as e:
            print(f"Embedding generation failed, using zeros: {e}")
            return np.zeros(768, dtype=np.float32)
    
    def upload_to_pinecone(self, tables: List[Dict]) -> bool:
        """Upload table embeddings to Pinecone"""
        if not self.pinecone_api_key:
            return False
            
        try:
            vectors = []
            for table in tables:
                # Generate embedding
                text_rep = self.create_table_text_representation(
                    table['table_title'],
                    table['headers'],
                    table['extracted_data']
                )
                embedding = self.generate_table_embedding(text_rep)
                
                metadata = {
                    "table_title": table['table_title'][:500],
                    "headers": json.dumps(table['headers'])[:1000],
                    "filing_id": table['filing_id'],
                    "num_rows": table['num_rows'],
                    "num_cols": table['num_cols']
                }
                
                vectors.append({
                    "id": f"{table['filing_id']}_{table['table_index']}",
                    "values": embedding.tolist(),
                    "metadata": metadata
                })
            
            # Upload in batches
            batch_size = 100
            for i in range(0, len(vectors), batch_size):
                batch = vectors[i:i + batch_size]
                self.index.upsert(vectors=batch)
            
            return True
            
        except Exception as e:
            print(f"Error uploading to Pinecone: {e}")
            return False
    
    def search_similar_tables(self, query: str, top_k: int = 20) -> List[Dict]:
        """Search for similar tables using Pinecone"""
        if not self.pinecone_api_key:
            return []
            
        try:
            # Generate query embedding
            query_embedding = self.generate_table_embedding(query)
            
            # Search Pinecone
            search_results = self.index.query(
                vector=query_embedding.tolist(),
                top_k=top_k,
                include_metadata=True
            )
            
            results = []
            for match in search_results.get('matches', []):
                results.append({
                    'score': match.score,
                    'table_id': match.id,
                    'metadata': match.metadata
                })
            
            return results
            
        except Exception as e:
            print(f"Search failed: {e}")
            return []

    def convert_to_document_url(self, index_url: str) -> str:
        """
        Convert SEC index URL to actual document URL
        
        Example:
        Input: https://www.sec.gov/Archives/edgar/data/731766/000073176624000340/0000731766-24-000340-index.htm
        Output: https://www.sec.gov/Archives/edgar/data/731766/000073176624000340/unh-20250630.htm
        """
        if not index_url:
            return index_url
            
        # Extract the accession number from the index URL
        # Pattern: /Archives/edgar/data/{cik}/{accession_number}/
        match = re.search(r'/Archives/edgar/data/(\d+)/([^/]+)/', index_url)
        if not match:
            return index_url
            
        cik, accession = match.groups()
        
        # For now, return the index URL and let the extraction process find the document links
        # The extraction process will follow the links to the actual documents
        return index_url
        
        return index_url
