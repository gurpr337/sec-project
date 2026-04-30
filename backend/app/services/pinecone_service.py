import os
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv
from ..config import settings

class PineconeService:
    """
    Service for managing semantic vector storage and retrieval using Pinecone.
    Handles indexing, upserting, and querying of financial table and metric embeddings.
    """

    def __init__(self):
        """
        Initializes the Pinecone client using settings from the configuration.
        
        Raises:
            ValueError: If PINECONE_API_KEY is not found in the environment.
        """
        self.api_key = settings.pinecone_api_key
        if not self.api_key:
            raise ValueError("PINECONE_API_KEY not found in environment variables")
        
        self.pc = Pinecone(api_key=self.api_key)
        self.index_name = "sec-tables-comprehensive"
        self.dimension = 3072

    def initialize_index(self) -> None:
        """
        Ensures the Pinecone index exists. If it doesn't, creates it with 
        the specified dimension and 'cosine' metric.
        """
        existing = [idx.name for idx in self.pc.list_indexes()]
        if self.index_name not in existing:
            self.pc.create_index(
                name=self.index_name,
                dimension=self.dimension,
                metric="cosine",
                spec=ServerlessSpec(cloud='aws', region='us-east-1') 
            )
        self.index = self.pc.Index(self.index_name)

    def upsert_vector(self, vector_id: str, vector: list[float], metadata: dict) -> None:
        """
        Upserts a vector and its metadata into the Pinecone index.

        Args:
            vector_id: A unique identifier for the vector.
            vector: The list of floats representing the embedding.
            metadata: Additional context to store with the vector.

        Raises:
            ValueError: If the vector dimension does not match the index dimension.
        """
        if not hasattr(self, 'index'):
            self.initialize_index()

        if len(vector) != self.dimension:
            raise ValueError(f"Vector dimension {len(vector)} does not match index dimension {self.dimension}")

        self.index.upsert(vectors=[
            {"id": vector_id, "values": vector, "metadata": metadata}
        ])

    def query_similar_tables(self, vector: list[float], top_k: int = 10) -> list[dict]:
        """
        Queries the index for tables with similar embeddings.

        Args:
            vector: The query embedding.
            top_k: The number of results to return.

        Returns:
            A list of matches with metadata.
        """
        if not hasattr(self, 'index'):
            self.initialize_index()
            
        results = self.index.query(
            vector=vector,
            top_k=top_k,
            include_metadata=True,
            filter={"type": {"$eq": "table_group"}}
        )
        return results['matches']

    def clear_all_embeddings(self) -> None:
        """
        Deletes and recreates the index, effectively clearing all data.
        Use with caution, primarily for development and testing.

        Raises:
            Exception: If the deletion or recreation fails.
        """
        try:
            if self.index_name in [idx.name for idx in self.pc.list_indexes()]:
                self.pc.delete_index(self.index_name)
                print(f"Deleted index: {self.index_name}")

            self.pc.create_index(
                name=self.index_name,
                dimension=self.dimension,
                metric="cosine",
                spec=ServerlessSpec(cloud='aws', region='us-east-1')
            )
            self.index = self.pc.Index(self.index_name)
            print(f"Recreated index: {self.index_name}")

        except Exception as e:
            print(f"Error clearing embeddings: {e}")
            raise

    def query_similar_metrics(self, vector: list[float], top_k: int = 10) -> list[dict]:
        """
        Queries the index for metrics with similar embeddings.

        Args:
            vector: The query embedding.
            top_k: The number of results to return.

        Returns:
            A list of matches with metadata.
        """
        if not hasattr(self, 'index'):
            self.initialize_index()
        
        results = self.index.query(
            vector=vector,
            top_k=top_k,
            include_metadata=True,
            filter={"type": {"$eq": "metric"}}
        )
        return results['matches']
