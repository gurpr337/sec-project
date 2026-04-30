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
                    headers = []
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
            
            return self.parse_html_tables(html_content, filing_id)
        except Exception as e:
            print(f"Error in extract_tables_from_html: {e}")
            return []
    
    def parse_html_tables(self, html_content: str, filing_id: str) -> List[Dict]:
        """Parse HTML content and extract tables"""
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = []
        
        for idx, table in enumerate(soup.find_all('table')):
            try:
                # Skip very small tables
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                # Extract table data
                table_data = {
                    'table_index': idx,
                    'filing_id': filing_id,
                    'raw_html': str(table),
                    'table_title': self.extract_table_title(table, soup),
                    'headers': self.extract_table_headers(table),
                    'extracted_data': self.extract_table_data(table),
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
        """Extract table title from surrounding context"""
        # Look for previous sibling elements
        prev = table.find_previous_sibling()
        attempts = 0
        
        while prev and attempts < 5:
            text = prev.get_text(strip=True)
            if text and len(text) > 5 and len(text) < 200:
                return text
            prev = prev.find_previous_sibling()
            attempts += 1
        
        # Check for caption
        caption = table.find('caption')
        if caption:
            return caption.get_text(strip=True)
        
        return "Untitled Table"
    
    def extract_table_headers(self, table) -> List[str]:
        """Extract table headers"""
        headers = []
        
        # Try thead first
        thead = table.find('thead')
        if thead:
            for th in thead.find_all(['th', 'td']):
                headers.append(th.get_text(strip=True))
        else:
            # Try first row
            first_row = table.find('tr')
            if first_row:
                for cell in first_row.find_all(['th', 'td']):
                    headers.append(cell.get_text(strip=True))
        
        return headers
    
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
        """Check if a row is a section header."""
        cells = row.find_all(['td', 'th'])
        if not cells:
            return None

        # Condition 1: The first cell must contain text.
        first_cell_text = cells[0].get_text(strip=True)
        if not first_cell_text:
            return None

        # Condition 2: All other cells in the row must be empty.
        for cell in cells[1:]:
            if cell.get_text(strip=True):
                return None # Not a header if other cells have content

        # Condition 3: Check for bold styling in the first cell.
        # This is a heuristic and might need to be adjusted.
        style = cells[0].find(style=True)
        if style and 'font-weight' in style['style']:
            try:
                font_weight = int(''.join(filter(str.isdigit, style['style'])))
                if font_weight > 400: # Standard font-weight for normal text is 400
                    return first_cell_text
            except (ValueError, IndexError):
                pass
        
        # Check for <strong> or <b> tags
        if cells[0].find(['strong', 'b']):
            return first_cell_text

        return None

    def extract_table_data(self, table: Tag) -> List[List[Dict]]:
        """Extract table data rows with cell coordinates"""
        data_rows = []
        
        current_section = "default"
        row_index_in_section = 0
        
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else table.find_all('tr')
        
        for row_idx, row in enumerate(rows):
            header_text = self._is_section_header(row)
            if header_text:
                current_section = header_text
                row_index_in_section = 0
                continue # Skip header rows from the data output

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
                    'row_index_in_section': row_index_in_section,
                    'unit_text': unit_text
                }
                cells.append(cell_data)
            
            if cells and any(c['text'] for c in cells):
                data_rows.append(cells)
                row_index_in_section += 1
        
        return data_rows
    
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
