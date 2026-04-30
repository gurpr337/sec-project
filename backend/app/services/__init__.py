from .embedding_service import EmbeddingService
from .pinecone_service import PineconeService
from .ingestion_service import IngestionService
from .table_grouping_service import TableGroupingService
from .metric_mapping_service import MetricMappingService

embedding_service = EmbeddingService()
pinecone_service = PineconeService()

table_grouping_service = TableGroupingService(
    embedding_service=embedding_service,
    pinecone_service=pinecone_service
)

metric_mapping_service = MetricMappingService(
    embedding_service=embedding_service,
    pinecone_service=pinecone_service
)

ingestion_service = IngestionService(
    table_grouping_service=table_grouping_service,
    metric_mapping_service=metric_mapping_service
)

# Utility function for clearing embeddings (useful for testing)
def clear_pinecone_embeddings():
    """Clear all embeddings from Pinecone index for testing"""
    return pinecone_service.clear_all_embeddings()
