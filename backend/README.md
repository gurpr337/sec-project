# SEC Filing Data Extraction (Backend)

The backend is a high-performance **FastAPI** service designed to orchestrate the discovery, extraction, and semantic analysis of SEC filings. It leverages a hybrid vector architecture and LLM-powered normalization to transform unstructured financial data into high-fidelity relational datasets.

## Technical Architecture Highlights

### 1. Dual-Layer Vector Strategy
This project employs a unique hybrid approach to vector storage to balance relational context with global search performance:
- **pgvector (PostgreSQL)**: Stores table-level embeddings directly within the relational database. This allows for "Local Context" queries—finding similar tables or structures within a specific company's history while maintaining strict foreign key integrity.
- **Pinecone (Vector DB)**: Used as the "Global Semantic Index." It stores metric-level and group-level embeddings to facilitate cross-company normalization and clustering at scale.

### 2. Semantic Normalization Pipeline
To handle the high variance in financial reporting (where different companies use different labels for the same concept), the backend implements a semantic ETL pipeline:
1. **Extraction**: BeautifulSoup-based parsing of multi-megabyte SEC HTML documents.
2. **Embedding**: Generating high-dimensional (3072-d) vectors using **Vertex AI (Gemini)**.
3. **Mapping**: Using vector similarity search to map raw labels (e.g., "Total Net Sales") to a canonical ontology (e.g., `revenue`).
4. **Storage**: Final persistence into PostgreSQL for structured querying.

### 3. Asynchronous Job Orchestration
Financial data extraction is computationally and network-intensive. The backend uses FastAPI's background tasks to:
- Deduplicate filings based on content hashing.
- Manage long-running ingestion jobs without blocking the main event loop.
- Provide a stateful API for the frontend to poll for granular progress updates.

## Core Services

- **`SECExtractor`**: Handles communication with the SEC EDGAR API and high-fidelity HTML table extraction.
- **`EmbeddingService`**: Manages connectivity to Vertex AI and handles rate-limiting for embedding generation.
- **`PineconeService`**: Abstracts the vector database operations for global semantic search.
- **`IngestionService`**: The primary orchestrator that coordinates the flow from raw HTML to normalized database records.

## Setup & Development

### Database Migrations
We use **Alembic** to manage the PostgreSQL schema.
```bash
# Run migrations
alembic upgrade head
```

### Running Tests
All functional tests and debug scripts are located in the `tests/` directory.
```bash
python -m tests.test_ingest_unh
```

### API Documentation
Once running, you can explore the interactive OpenAPI documentation at `http://localhost:8000/docs`.
