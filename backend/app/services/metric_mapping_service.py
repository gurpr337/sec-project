import re
from sqlalchemy.orm import Session
from ..models import CanonicalMetric
from .embedding_service import EmbeddingService
from .pinecone_service import PineconeService

class MetricMappingService:
    def __init__(self, embedding_service: EmbeddingService, pinecone_service: PineconeService):
        self.embedding_service = embedding_service
        self.pinecone_service = pinecone_service

    def get_or_create_canonical_metric(self, db: Session, flattened_name: str, us_gaap_tag: str = None) -> CanonicalMetric:
        # 1. Try to find by GAAP tag first (most reliable)
        if us_gaap_tag:
            canonical_metric = db.query(CanonicalMetric).filter(CanonicalMetric.us_gaap_tag == us_gaap_tag).first()
            if canonical_metric:
                return canonical_metric

        # 2. Try to find by exact flattened name
        canonical_metric = db.query(CanonicalMetric).filter(CanonicalMetric.flattened_name == flattened_name).first()
        if canonical_metric:
            return canonical_metric

        # 3. Try to find by semantic similarity
        embedding = self.embedding_service.get_embedding(flattened_name)
        if embedding:
            similar_metrics = self.pinecone_service.query_similar_metrics(embedding)
            if similar_metrics and similar_metrics[0]['score'] > 0.95:
                metric_id = similar_metrics[0]['metadata']['canonical_metric_id']
                canonical_metric = db.query(CanonicalMetric).filter(CanonicalMetric.id == metric_id).first()
                if canonical_metric:
                    return canonical_metric

        # 4. Create a new one
        new_canonical_metric = CanonicalMetric(
            flattened_name=flattened_name,
            us_gaap_tag=us_gaap_tag,
            embedding=embedding
        )
        db.add(new_canonical_metric)
        db.commit()
        db.refresh(new_canonical_metric)

        # Add to pinecone for future lookups
        if embedding:
            metadata = {
                'canonical_metric_id': new_canonical_metric.id,
                'flattened_name': new_canonical_metric.flattened_name
            }
            self.pinecone_service.upsert_vector(f"metric_{new_canonical_metric.id}", embedding, metadata)

        return new_canonical_metric
