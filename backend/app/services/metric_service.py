import numpy as np
from sqlalchemy.orm import Session
from ..models import DocumentTable, MetricValue, ColumnHeader
from .embedding_service import EmbeddingService
from .pinecone_service import PineconeService
from .. import models
import json
from typing import List, Dict, Tuple, Optional

class MetricService:
    def __init__(self, embedding_service: EmbeddingService, pinecone_service: PineconeService):
        self.embedding_service = embedding_service
        self.pinecone_service = pinecone_service

    def process_table_metrics(self, db: Session, document_table: DocumentTable):
        if not document_table.table_group_id:
            raise Exception("Table must be assigned to a group before processing metrics.")

        table_data = document_table.extracted_data or []
        if not table_data:
            return

        header_row_index, column_headers = self._get_or_create_column_headers(db, document_table.table_group_id, table_data)
        
        start_row = header_row_index + 1 if header_row_index is not None else 0

        for row_idx, row in enumerate(table_data[start_row:]):
            if not row:
                continue

            raw_metric_text = row[0]['text'].strip() if isinstance(row[0], dict) and 'text' in row[0] else str(row[0]).strip()
            if not raw_metric_text:
                continue

            canonical_metric_name = self._get_or_create_canonical_metric_name(
                db, document_table.table_group_id, raw_metric_text, row[0].get('section_header')
            )

            for col_idx, cell in enumerate(row[1:]):
                if col_idx + 1 not in column_headers:
                    continue # Skip columns that are not identified as headers

                value_text = cell['text'].strip() if isinstance(cell, dict) and 'text' in cell else str(cell).strip()
                try:
                    value = float(value_text.replace(',', '').replace('$', '').replace('(', '-').replace(')', ''))
                except ValueError:
                    continue

                metric_value = MetricValue(
                    company_id=document_table.document.company_id,
                    source_table_id=document_table.id,
                    table_group_id=document_table.table_group_id,
                    column_header_id=column_headers[col_idx + 1].id,
                    canonical_metric_name=canonical_metric_name,
                    original_label=raw_metric_text,
                    value=value,
                    unit_text=cell.get('unit_text'),
                    section_header=cell.get('section_header'),
                    row_index_in_section=cell.get('row_index_in_section'),
                    filing_date=document_table.document.filing_date,
                    cell_coordinates=cell.get('coordinates', {})
                )
                db.add(metric_value)
        
        db.commit()

    def _get_or_create_canonical_metric_name(self, db: Session, table_group_id: int, raw_metric_text: str, section_header: str) -> str:
        # Create a context-aware string for embedding
        embedding_text = f"Section: {section_header} - Metric: {raw_metric_text}"
        embedding = self.embedding_service.get_embedding(embedding_text)
        if not embedding:
            raise Exception(f"Failed to generate embedding for metric: {raw_metric_text}")

        similar_metrics = self.pinecone_service.query_similar_labels(embedding, table_group_id=table_group_id)

        if similar_metrics and similar_metrics[0]['score'] > 0.95:
            return similar_metrics[0]['metadata']['canonical_metric_name']

        # This is a new metric, so the raw text becomes the canonical name
        canonical_name = raw_metric_text
        
        # Increment the metric_count for the table group
        table_group = db.query(models.TableGroup).filter(models.TableGroup.id == table_group_id).first()
        if table_group:
            table_group.metric_count = (table_group.metric_count or 0) + 1
            db.commit()
            
        pinecone_id = ''.join(c if c.isascii() else '_' for c in canonical_name)

        self.pinecone_service.upsert_label(
            label_id=f"metric_{table_group_id}_{pinecone_id}",
            vector=embedding,
            metadata={'canonical_metric_name': canonical_name, 'table_group_id': table_group_id}
        )
        
        return canonical_name

    def _get_or_create_column_headers(self, db: Session, table_group_id: int, table_data: List[List[Dict]]) -> Tuple[Optional[int], Dict[int, models.ColumnHeader]]:
        """Identifies the header row and returns its index and a mapping of column_index -> ColumnHeader."""
        header_map = {}
        # Check the first 5 rows for a likely header
        for row_idx, row in enumerate(table_data[:5]):
            is_header_row = True
            temp_header_map = {}

            # Heuristic checks
            if len(row) < 2: continue
            
            # Check for bolding and that other cells are not just numbers
            # A simple heuristic for a header row: multiple cells, not the first column, and some bold text
            if len(row) > 1 and any('font-weight:700' in cell.get('raw_html', '') for cell in row[1:]):
                for col_idx, cell in enumerate(row):
                    if col_idx == 0: continue # Skip the parameter column
                    
                    header_text = cell['text'].strip()
                    if not header_text: continue

                    embedding = self.embedding_service.get_embedding(header_text)
                    similar_headers = self.pinecone_service.query_similar_labels(embedding, table_group_id=table_group_id)
                    
                    if similar_headers and similar_headers[0]['score'] > 0.95:
                        header = db.query(models.ColumnHeader).filter(models.ColumnHeader.id == similar_headers[0]['metadata']['column_header_id']).first()
                    else:
                        header = models.ColumnHeader(
                            table_group_id=table_group_id,
                            header_text=header_text,
                            header_embedding=embedding
                        )
                        db.add(header)
                        db.commit()
                        db.refresh(header)
                        
                        pinecone_id = ''.join(c if c.isascii() else '_' for c in header_text)
                        self.pinecone_service.upsert_label(
                            label_id=f"column_{table_group_id}_{pinecone_id}",
                            vector=embedding,
                            metadata={'column_header_id': header.id, 'table_group_id': table_group_id}
                        )
                    temp_header_map[col_idx] = header

            # If it looks like a header row, process it
            if temp_header_map:
                return row_idx, temp_header_map
                
        return None, {}
