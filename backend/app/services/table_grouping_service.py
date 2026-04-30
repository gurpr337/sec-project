import json
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from ..models import FinancialTable, FinancialTableGroup, ColumnHeader, FinancialMetric
from .embedding_service import EmbeddingService
from .pinecone_service import PineconeService

class TableGroupingService:
    def __init__(self, embedding_service: EmbeddingService, pinecone_service: PineconeService):
        self.embedding_service = embedding_service
        self.pinecone_service = pinecone_service

    def get_or_create_table_group(self, db: Session, table: FinancialTable, column_headers: List[Dict],
                                 sections: List[str] = None, num_rows: int = 0, num_cols: int = 0) -> FinancialTableGroup:
        """Enhanced table grouping with multi-factor similarity scoring"""

        # Create comprehensive table representation
        table_data = {
            'title': table.title or '',
            'column_headers': [h.get('flattened_name', h.get('raw_name', '')) for h in column_headers],
            'sections': sections or [],
            'num_rows': num_rows,
            'num_cols': num_cols,
            'doc_type': self._extract_doc_type_from_table(table)
        }

        # Generate embedding from comprehensive representation
        text_rep = self._create_comprehensive_table_representation(table_data)
        embedding = self.embedding_service.get_embedding(text_rep)
        if embedding is None or len(embedding) == 0:
            raise Exception("Failed to generate embedding for the table")

        table.embedding = embedding
        db.commit()

        # Find candidate similar groups using embedding similarity
        candidate_groups = self._find_candidate_groups(embedding, db)

        # Apply multi-factor similarity scoring
        best_match = self._select_best_group_match(table_data, candidate_groups, db)

        if best_match:
            table.table_group_id = best_match.id
            db.commit()
            return best_match

        return self._create_new_table_group(db, table)

    def _create_new_table_group(self, db: Session, table: FinancialTable) -> FinancialTableGroup:
        group_name = table.title or "Untitled Table"

        # Check if a group with this name already exists
        existing_group = db.query(FinancialTableGroup).filter(FinancialTableGroup.name == group_name).first()
        if existing_group:
            # Use existing group instead of creating duplicate
            table.table_group_id = existing_group.id
            db.commit()
            return existing_group

        group = FinancialTableGroup(
            name=group_name,
        )
        db.add(group)
        db.commit()
        db.refresh(group)

        table.table_group_id = group.id
        db.commit()

        metadata = {
            'group_id': group.id,
            'table_title': table.title,
        }
        self.pinecone_service.upsert_vector(f"table_group_{group.id}", table.embedding, metadata)

        return group

    def _extract_doc_type_from_table(self, table: FinancialTable) -> str:
        """Extract document type from table's document"""
        if table.document:
            return table.document.form_type or 'unknown'
        return 'unknown'

    def _create_comprehensive_table_representation(self, table_data: Dict) -> str:
        """Create comprehensive text representation for embedding"""
        parts = [
            f"Title: {table_data['title']}",
            f"Document Type: {table_data['doc_type']}",
            f"Dimensions: {table_data['num_rows']} rows x {table_data['num_cols']} columns"
        ]

        if table_data['column_headers']:
            parts.append(f"Column Headers: {', '.join(table_data['column_headers'])}")

        if table_data['sections']:
            parts.append(f"Sections: {', '.join(table_data['sections'])}")

        return '\n'.join(parts)

    def _find_candidate_groups(self, embedding: List[float], db: Session) -> List[FinancialTableGroup]:
        """Find candidate groups using initial embedding similarity"""
        candidates = []

        # Query pinecone for similar tables
        similar_tables = self.pinecone_service.query_similar_tables(embedding)

        # Get unique group IDs from similar tables
        group_ids = set()
        for result in similar_tables:
            if result['score'] > 0.7:  # Lower threshold for candidates
                group_ids.add(result['metadata']['group_id'])

        # Fetch candidate groups
        if group_ids:
            candidates = db.query(FinancialTableGroup).filter(
                FinancialTableGroup.id.in_(group_ids)
            ).all()

        return candidates

    def _select_best_group_match(self, table_data: Dict, candidate_groups: List[FinancialTableGroup],
                                db: Session) -> Optional[FinancialTableGroup]:
        """Apply multi-factor similarity scoring to select best group match"""

        best_group = None
        best_score = 0.0

        for group in candidate_groups:
            # Get a representative table from this group to compare against
            representative_table = db.query(FinancialTable).filter(
                FinancialTable.table_group_id == group.id
            ).first()

            if not representative_table:
                continue

            # Calculate multi-factor similarity score
            similarity_score = self._calculate_multi_factor_similarity(table_data, representative_table, db)

            if similarity_score > best_score and similarity_score > 0.85:  # High confidence threshold
                best_score = similarity_score
                best_group = group

        return best_group

    def _calculate_multi_factor_similarity(self, table_data: Dict, existing_table: FinancialTable, db: Session) -> float:
        """Calculate similarity using multiple factors"""

        # Factor 1: Title similarity (25% weight)
        title_sim = self._calculate_text_similarity(
            table_data['title'],
            existing_table.title or ''
        )

        # Factor 2: Document type match (20% weight)
        doc_type_match = 1.0 if table_data['doc_type'] == self._extract_doc_type_from_table(existing_table) else 0.0

        # Factor 3: Column header similarity (25% weight)
        existing_headers = db.query(ColumnHeader).filter(
            ColumnHeader.table_id == existing_table.id
        ).all()
        existing_header_names = [h.flattened_name for h in existing_headers]
        col_header_sim = self._calculate_list_similarity(
            table_data['column_headers'],
            existing_header_names
        )

        # Factor 4: Section similarity (15% weight)
        # Note: We'll need to get sections from existing table's metrics
        existing_sections = self._extract_sections_from_table(existing_table, db)
        section_sim = self._calculate_list_similarity(
            table_data['sections'],
            existing_sections
        )

        # Factor 5: Dimensional similarity (10% weight)
        existing_metrics_count = db.query(FinancialMetric).filter(
            FinancialMetric.table_id == existing_table.id
        ).count()
        existing_cols_count = len(existing_headers)

        dim_sim = self._calculate_dimensional_similarity(
            table_data['num_rows'], table_data['num_cols'],
            existing_metrics_count, existing_cols_count
        )

        # Factor 6: Embedding similarity (5% weight) - from pinecone
        embedding_sim = 0.0
        if existing_table.embedding is not None and len(existing_table.embedding) > 0:
            embedding_sim = self._calculate_embedding_similarity(
                table_data.get('embedding', []),
                existing_table.embedding
            )

        # Weighted average
        similarity = (
            title_sim * 0.25 +
            doc_type_match * 0.20 +
            col_header_sim * 0.25 +
            section_sim * 0.15 +
            dim_sim * 0.10 +
            embedding_sim * 0.05
        )

        return similarity

    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """Calculate text similarity using simple methods"""
        if not text1 and not text2:
            return 1.0
        if not text1 or not text2:
            return 0.0

        # Simple word overlap similarity
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())

        if not words1 and not words2:
            return 1.0
        if not words1 or not words2:
            return 0.0

        intersection = words1.intersection(words2)
        union = words1.union(words2)

        return len(intersection) / len(union)

    def _calculate_list_similarity(self, list1: List[str], list2: List[str]) -> float:
        """Calculate similarity between two lists"""
        if not list1 and not list2:
            return 1.0
        if not list1 or not list2:
            return 0.0

        # Simple Jaccard similarity for list elements
        set1 = set(str(x).lower() for x in list1)
        set2 = set(str(x).lower() for x in list2)

        intersection = set1.intersection(set2)
        union = set1.union(set2)

        return len(intersection) / len(union) if union else 0.0

    def _calculate_dimensional_similarity(self, rows1: int, cols1: int, rows2: int, cols2: int) -> float:
        """Calculate dimensional similarity"""
        # Simple ratio-based similarity with tolerance
        row_ratio = min(rows1, rows2) / max(rows1, rows2) if max(rows1, rows2) > 0 else 1.0
        col_ratio = min(cols1, cols2) / max(cols1, cols2) if max(cols1, cols2) > 0 else 1.0

        return (row_ratio + col_ratio) / 2.0

    def _calculate_embedding_similarity(self, emb1: List[float], emb2: List[float]) -> float:
        """Calculate cosine similarity between embeddings"""
        if not emb1 or not emb2 or len(emb1) != len(emb2):
            return 0.0

        # Cosine similarity
        dot_product = sum(a * b for a, b in zip(emb1, emb2))
        norm1 = sum(a * a for a in emb1) ** 0.5
        norm2 = sum(b * b for b in emb2) ** 0.5

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot_product / (norm1 * norm2)

    def _extract_sections_from_table(self, table: FinancialTable, db: Session) -> List[str]:
        """Extract section names from a table's metrics"""
        # This is a simplified version - in reality we'd need to analyze the metrics
        # to identify section headers. For now, return empty list.
        return []

    def _create_table_text_representation(self, table: FinancialTable, column_headers: List[Dict]) -> str:
        """Legacy method for backward compatibility"""
        return self._create_comprehensive_table_representation({
            'title': table.title or '',
            'column_headers': [h.get('flattened_name', h.get('raw_name', '')) for h in column_headers],
            'sections': [],
            'num_rows': 0,
            'num_cols': len(column_headers),
            'doc_type': self._extract_doc_type_from_table(table)
        })
