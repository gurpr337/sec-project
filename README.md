# SEC Filing Data Extraction System

![Dashboard Preview](assets/dashboard_preview.png)

A comprehensive full-stack application for extracting, processing, and analyzing financial data from SEC filings using AI-powered semantic similarity and vector embeddings.

[**View Backend Deep Dive**](backend/README.md) | [**View Frontend Deep Dive**](frontend/README.md)

## System Overview

This system extracts financial metrics from SEC filings (8-K, 10-K, 10-Q) by:
1. **Fetching filings** from the SEC API
2. **Parsing HTML tables** from filing documents
3. **Generating embeddings** using Vertex AI's Gemini model
4. **Storing vectors** in Pinecone for semantic similarity
5. **Mapping metrics** to canonical names for consistency
6. **Storing structured data** in PostgreSQL
7. **Visualizing trends** in an interactive frontend

## Architecture

```mermaid
graph TD
    subgraph "External Data"
        SEC["SEC EDGAR API"]
    end

    subgraph "Backend (FastAPI & Python 3.10+)"
        Extractor["Extraction Service<br/>(BeautifulSoup Parser)"]
        LLM["Semantic Mapping<br/>(Vertex AI / Gemini)"]
        Storage["Persistence Layer<br/>(PostgreSQL)"]
    end

    subgraph "Vector Intelligence"
        Pinecone[(Pinecone Cloud<br/>Vector Database)]
    end

    subgraph "Frontend (Next.js & Recharts)"
        Dashboard["Financial Dashboard"]
    end

    %% Data Flow
    SEC -->|Raw HTML| Extractor
    Extractor -->|Table Content| LLM
    LLM <-->|Similarity Search| Pinecone
    LLM -->|Canonical Mapping| Storage
    Storage -->|JSON API| Dashboard
    Dashboard -->|Visualization| Dashboard
```

### Frontend (Next.js + React)
- **Location**: `sec-project/frontend/src/pages/index.js`
- **Purpose**: User interface for company management, extraction control, and data visualization
- **Key Features**: Company selection, date range picker, real-time extraction progress, interactive charts

### Backend (FastAPI + SQLAlchemy)
- **Location**: `sec-project/backend/app/`
- **Purpose**: API server handling extraction jobs, data processing, and storage
- **Key Services**: SEC API integration, embedding generation, metric mapping, database management

### Database (PostgreSQL)
- **Purpose**: Relational storage for companies, documents, tables, and metric values
- **Tables**: `companies`, `documents`, `tables`, `metric_values`, `extraction_jobs`

### Vector Database (Pinecone)
- **Purpose**: Semantic similarity search for tables and metric labels
- **Index**: `sec-tables-comprehensive` (3072-dimensional vectors)

## Quick Start

### Prerequisites
- **Python 3.8+** (for backend)
- **Node.js 16+** (for frontend)
- **PostgreSQL** database (for data storage)
- **SEC API key** (from [sec-api.com](https://sec-api.com))
- **Pinecone API key** (from [pinecone.io](https://pinecone.io))
- **Google Cloud credentials** for Vertex AI (for embeddings)

### Key Dependencies
- **Backend**: FastAPI, SQLAlchemy, Vertex AI, Pinecone, SEC API
- **Frontend**: Next.js 15.5.2, React 19.1.0, Recharts 3.1.2, Tailwind CSS
- **Database**: PostgreSQL with Alembic migrations

### Backend Setup

1. **Navigate to backend directory:**
   ```bash
   cd sec-project/backend
   ```

2. **Install task runner (optional but recommended):**
   ```bash
   # macOS
   brew install go-task
   
   # Linux
   sudo snap install task --classic
   
   # Windows
   # Download from https://taskfile.dev/installation/
   ```

3. **Create and activate virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

4. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

5. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and database URL
   ```

6. **Start the backend server:**
   ```bash
   # Using task runner (recommended)
   task run
   
   # Or directly with Python
   python run.py
   ```

The backend will be available at `http://localhost:8000`

### Frontend Setup

1. **Navigate to frontend directory:**
   ```bash
   cd sec-project/frontend
   ```

2. **Install Node.js dependencies:**
   ```bash
   npm install
   ```

3. **Start the development server:**
   ```bash
   npm run dev
   ```

The frontend will be available at `http://localhost:3000`

### Environment Variables Setup

Copy and configure the environment file:
```bash
cd sec-project/backend
cp .env.example .env
```

Edit `.env` with your actual values:
```bash
# Database Configuration
   DATABASE_URL=postgresql://username:password@localhost/sec_project

# SEC API Configuration (get from sec-api.com)
SEC_API_KEY=your_sec_api_key_here
SEC_EXTRACTOR_API_KEY=your_sec_extractor_api_key_here

# Pinecone Configuration (get from pinecone.io)
PINECONE_API_KEY=your_pinecone_api_key_here

# Vertex AI / Google Cloud (for embeddings)
VERTEXAI_PROJECT=your-gcp-project-id
VERTEXAI_LOCATION=us-central1

# BigQuery Configuration (optional)
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
```

### Database Management

**Wipe all data (useful for testing):**
```bash
cd sec-project/backend
task wipe-db
```

**View available tasks:**
```bash
task --list
```

**Run database migrations:**
```bash
# If using task runner
task migrate

# Or directly with alembic
alembic upgrade head
```

## Complete Data Flow

### 1. Company Setup
**Frontend** (`frontend/pages/index.js`): Users add a company via ticker and name.
**Backend** (`backend/app/routers/companies.py`): Persists the company to PostgreSQL (`sec_app.companies`).

### 2. Extraction Job Initiation
**Frontend**: Triggers the extraction for specific form types (8-K, 10-Q, 10-K) and date ranges.
**Backend** (`backend/app/routers/extraction.py`):
1. Creates a record in `sec_app.extraction_jobs`.
2. Initiates a background task using `run_extraction_job`.
3. The frontend polls `/api/extraction/jobs/{id}` for real-time progress updates.

### 3. Filing Retrieval & Extraction
**SECExtractor** (`backend/app/services/sec_extractor.py`):
1. Queries the SEC EDGAR API for filings matching the criteria.
2. Downloads the HTML filing and extracts financial tables using BeautifulSoup.
3. Performs content-hash deduplication to ensure data integrity.

### 4. Semantic Processing & Mapping
**IngestionService** (`backend/app/services/ingestion_service.py`):
1. **Embedding**: Generates 3072-d vectors for tables and labels using **Vertex AI (Gemini)**.
2. **Table Storage**: Persists tables with embeddings in `sec_app.financial_tables` (using **pgvector**).
3. **Metric Mapping**: Maps raw table labels to canonical metrics using **Pinecone** similarity search and the **MetricMappingService**.
4. **Data Persistence**: Stores structured data across `financial_metrics`, `column_headers`, and `data_points`.

### 5. Visualization
**Frontend**: Fetches normalized time-series data and renders interactive charts using **Recharts**, allowing users to trace data points back to their original source in the SEC filing.

## Database Schema (PostgreSQL)

The system uses a highly relational schema under the `sec_app` namespace:

- **`companies`**: Ticker, name, and metadata.
- **`documents`**: SEC filing metadata (accession number, form type, filing date, URL).
- **`financial_tables`**: Extracted tables with **pgvector** embeddings for semantic lookup.
- **`financial_metrics`**: Flattened metric names with hierarchical relationships.
- **`column_headers`**: Normalized time-period headers.
- **`data_points`**: The core values linked to metrics, headers, and source coordinates.
- **`extraction_jobs`**: Tracking for background ingestion tasks.

## Key Features

- **Hybrid Vector Search**: Uses `pgvector` for local relational similarity and `Pinecone` for global metric normalization.
- **AI-Powered Extraction**: BeautifulSoup and Gemini-based mapping to handle the high variance in SEC reporting.
- **Real-time Feedback**: Granular job status polling with progress bars and event logs.
- **Source Traceability**: Ability to click a data point in a chart and view the exact source cell in the original SEC HTML filing.

## API Endpoints

### Companies
- `GET /api/companies/` - List all companies
- `POST /api/companies/` - Create new company

### Extraction
- `POST /api/extraction/start` - Start background extraction job
- `GET /api/extraction/jobs/{job_id}` - Get real-time job status
- `GET /api/documents/{company_ticker}` - List processed filings

### Financial Data
- `GET /api/financial-tables/groups/{id}/evolution` - Get normalized time-series data
- `GET /api/financial-tables/companies/{ticker}/groups` - List discovered table groups

## Project Structure

```
sec-project/
├── backend/
│   ├── app/
│   │   ├── models/          # SQLAlchemy schemas (PostgreSQL + pgvector)
│   │   ├── routers/         # FastAPI endpoints (Extraction, Companies, etc.)
│   │   ├── services/        # Logic (AI Embeddings, SEC Parsing, Pinecone)
│   │   └── tests/           # Functional tests and debug scripts
│   ├── Taskfile.yml         # Automation for migrations and runs
│   └── requirements.txt     # Python dependencies
├── frontend/
│   ├── pages/               # Next.js pages and dashboard logic
│   ├── public/              # Static assets
│   └── package.json         # Node.js dependencies
├── assets/                  # Documentation images
└── README.md
```

## Troubleshooting

1. **Database Connection**: Ensure PostgreSQL is running and `DATABASE_URL` in `backend/.env` is correct.
2. **API Quotas**: Monitor Vertex AI and SEC API quotas if you encounter 429 errors.
3. **Pinecone Index**: Ensure your index dimension is set to **3072** to match the Gemini embedding model output.

## License

This project is licensed under the [MIT License](LICENSE).

