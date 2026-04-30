# SEC Filing Data Extraction System

![Dashboard Preview](dashboard_preview.png)

A comprehensive full-stack application for extracting, processing, and analyzing financial data from SEC filings using AI-powered semantic similarity and vector embeddings.

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
    LLM -->|Vector Embeddings| Pinecone
    Extractor -->|Structured Data| Storage
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

## Complete Data Flow

### 1. Company Selection and Setup

**Frontend** (`sec-project/frontend/src/pages/index.js`):
```javascript
// User adds a company with ticker and name
const addCompany = async () => {
  const res = await fetch(`${API_BASE}/companies/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ticker: addTicker, name: addName })
  })
  const created = await res.json()
  setCompanies(prev => [...prev, created])
  setSelectedCompany(created)
}
```

**Backend** stores the company in PostgreSQL:
```sql
-- sec-project/backend/app/models/company.py
CREATE TABLE companies (
    id INTEGER PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    cik VARCHAR(20),
    -- ... other fields
);
```

### 2. Extraction Job Initiation

**Frontend** triggers extraction:
```javascript
// sec-project/frontend/src/pages/index.js:127-160
const startExtraction = async () => {
  const url = `${API_BASE}/extraction/start?company_ticker=${encodeURIComponent(selectedCompany.ticker)}&form_types=${encodeURIComponent('8-K,10-Q,10-K')}&start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(endDate)}`
  const res = await fetch(url, { method: 'POST' })
  const job = await res.json()
  setJobId(job.id)
  setJobStatus('running')
  // Begin polling for progress updates every 3 seconds
}
```

**Backend** creates extraction job:
```python
# sec-project/backend/app/routers/extraction.py:32-65
@router.post("/start")
async def start_extraction_job(
    company_ticker: str,
    form_types: str,  # "8-K,10-Q,10-K"
    start_date: str,
    end_date: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    # Create extraction job record
    job = ExtractionJob(
        company_id=company.id,
        status="pending",
        job_type="initial",
        job_metadata={
            "company_ticker": company.ticker,
            "form_types": form_types_list,
            "start_date": start_date,
            "end_date": end_date,
        }
    )
    db.add(job)
    db.commit()
    
    # Start background processing
    background_tasks.add_task(run_extraction_job_simple, job.id)
    return job
```

### 3. SEC Filing Retrieval

**Backend** fetches filings from SEC API:
```python
# sec-project/backend/app/routers/extraction.py:100-115
sec_extractor = SECExtractor()
filings = sec_extractor.get_filings(
    ticker=metadata["company_ticker"],
    form_types=metadata["form_types"],
    start_date=metadata["start_date"],
    end_date=metadata["end_date"]
)

# Create document records for each filing
for filing in filings:
    doc = Document(
        company_id=company.id,
        accession_number=filing.get('accessionNo'),
        form_type=filing.get('formType'),
        filing_date=filing_date_val,
        file_url=filing.get('linkToHtml')
    )
    db.add(doc)
```

### 4. Table Extraction and Processing

**Backend** extracts tables from HTML documents:
```python
# sec-project/backend/app/routers/extraction.py:125-155
tables_data = sec_extractor.extract_tables_from_filing(doc.file_url, doc.accession_number)

for table_data in tables_data:
    # Check for duplicate tables using content hash
    existing_table = db.query(Table).filter(
        Table.document_id == doc.id,
        Table.content_hash == table_data.get('content_hash')
    ).first()
    
    if not existing_table:
        # Create new table record
        table_record = Table(
            document_id=doc.id,
            table_index=table_data.get('table_index', 0),
            table_title=table_data.get('table_title'),
            headers=json.dumps(table_data.get('headers')),
            extracted_data=json.dumps(table_data.get('extracted_data')),
            content_hash=table_data.get('content_hash')
        )
        db.add(table_record)
        db.commit()
```

### 5. Embedding Generation with Vertex AI

**Backend** generates embeddings for tables and metric labels:
```python
# sec-project/backend/app/services/embedding_service.py:140-180
class EmbeddingService:
    def __init__(self):
        # Initialize Vertex AI with Gemini Embedding model
        vertexai.init(project=self.vertex_project, location=self.vertex_location)
        self.vertex_model = TextEmbeddingModel.from_pretrained(VertexAIModel.GEMINI_EMBEDDING)
        # GEMINI_EMBEDDING outputs 3072-dimensional vectors
        self.provider = "vertex"
    
    def get_embedding(self, text: str) -> list[float]:
        # Rate limiting: 0.3 seconds between calls
        time.sleep(0.3)
        
        # Generate embedding using Vertex AI
        embeddings = self.vertex_model.get_embeddings([text])
        values = embeddings[0].values
        return [float(x) for x in values]
```

**Table embedding generation**:
```python
# sec-project/backend/app/routers/extraction.py:190-210
# Build text for table embedding
text_parts = [
    f"Title: {table_record.table_title}",
    f"Headers: {', '.join(headers_list)}"
] + preview_rows
text_to_embed = "\n".join(text_parts)

# Generate embedding
vector = embedding_service.get_embedding(text_to_embed)

# Store in Pinecone
if vector and pinecone_service is not None:
    pinecone_metadata = {
        "company_ticker": company.ticker,
        "document_id": doc.id,
        "form_type": doc.form_type,
        "filing_date": doc.filing_date.isoformat()
    }
    pinecone_service.upsert_vector(table_record.id, vector, pinecone_metadata)
```

### 6. Vector Storage in Pinecone

**Backend** stores embeddings in Pinecone:
```python
# sec-project/backend/app/services/pinecone_service.py:15-45
class PineconeService:
    def __init__(self):
        self.index_name = "sec-tables-comprehensive"
        self.dimension = 3072  # GEMINI_EMBEDDING outputs 3072-d vectors
    
    def upsert_vector(self, table_id: int, vector: list[float], metadata: dict):
        # Ensure vector matches index dimension
        vec = vector or []
        if len(vec) < self.dimension:
            vec = vec + [0.0] * (self.dimension - len(vec))
        elif len(vec) > self.dimension:
            vec = vec[:self.dimension]
        
        self.index.upsert(vectors=[
            {"id": str(table_id), "values": vec, "metadata": metadata}
        ])
```

### 7. Metric Extraction and Canonical Mapping

**Backend** processes table rows into metrics:
```python
# sec-project/backend/app/routers/extraction.py:240-290
for row in extracted_rows:
    label = row[0]  # First column is the metric label
    
    # Generate label embedding for semantic grouping
    label_embedding_text = f"Metric label: {label}. Context headers: {', '.join(headers)}. Table: {table_record.table_title or ''}."
    label_vec = embedding_service.get_embedding(label_embedding_text)
    
    # Find similar existing labels using Pinecone
    if pinecone_service is not None and label_vec:
        matches = pinecone_service.query_similar_labels(label_vec, company_id=company.id, top_k=5)
        if matches and matches[0]['score'] >= 0.88:
            canonical_name = matches[0]['metadata'].get('canonical_key')
    
    # Fallback to mapping service
    if not canonical_name:
        canonical_name = metric_mapping_service.map_label_to_canonical(label)
    
    # Store label embedding for future similarity search
    if canonical_name and pinecone_service is not None and label_vec:
        pinecone_service.upsert_label(
            label_id=f"label:{company.id}:{table_record.id}:{col_idx}",
            vector=label_vec,
            metadata={
                "type": "label",
                "company_id": int(company.id),
                "canonical_key": canonical_name,
                "original_label": str(label)
            }
        )
```

### 8. Metric Value Storage in PostgreSQL

**Backend** stores extracted metrics:
```python
# sec-project/backend/app/routers/extraction.py:300-330
for col_idx in range(1, len(row)):
    value_str = row[col_idx]
    numeric_value = metric_mapping_service.parse_financial_value(value_str, multiplier=units_multiplier)
    
    # Check for duplicate metrics
    existing_metric = db.query(models.MetricValue).filter(
        models.MetricValue.company_id == company.id,
        models.MetricValue.source_table_id == table_record.id,
        models.MetricValue.canonical_metric_name == canonical_name,
        models.MetricValue.filing_date == period_date
    ).first()
    
    if not existing_metric:
        metric = models.MetricValue(
            company_id=company.id,
            source_table_id=table_record.id,
            canonical_metric_name=canonical_name,
            original_label=str(label),
            value=numeric_value,
            unit_text=units_text,
            unit_multiplier=units_multiplier,
            filing_date=period_date
        )
        db.add(metric)
```

### 9. Frontend Data Visualization

**Frontend** loads and displays metrics:
```javascript
// sec-project/frontend/src/pages/index.js:45-75
// Load available metrics for selected company
useEffect(() => {
  if (!selectedCompany) return
  fetch(`${API_BASE}/metrics/${encodeURIComponent(selectedCompany.ticker)}/list`)
    .then(r => r.ok ? r.json() : [])
    .then(list => setAvailableMetrics(list))
}, [selectedCompany])

// Load time series data for selected metrics
useEffect(() => {
  if (!selectedCompany) return
  const loadMissing = async () => {
    for (const m of selectedMetrics) {
      const r = await fetch(`${API_BASE}/metrics/${encodeURIComponent(selectedCompany.ticker)}/${encodeURIComponent(m)}`)
      const arr = await r.json()
      setSeriesByMetric(prev => ({ ...prev, [m]: arr }))
    }
  }
  loadMissing()
}, [selectedCompany, selectedMetrics])
```

**Interactive chart rendering**:
```javascript
// sec-project/frontend/src/pages/index.js:280-320
<ResponsiveContainer width="100%" height="100%">
  <LineChart data={combinedChartData}>
    <XAxis dataKey="date" />
    <YAxis />
    <Tooltip content={<CustomTooltip selectedMetrics={selectedMetrics} tableIdByMetricDate={tableIdByMetricDate} />} />
    <Legend />
    {selectedMetrics.map((m, idx) => (
      <Line 
        key={m} 
        type="monotone" 
        dataKey={m} 
        stroke={palette[idx % palette.length]} 
        strokeWidth={3} 
        dot={{ fill: palette[idx % palette.length], strokeWidth: 2, r: 4 }} 
      />
    ))}
    {hoverDetails && !isPinned && (
      <ReferenceLine
        x={hoverDetails.date}
        stroke="#666"
        strokeDasharray="3 3"
        strokeWidth={2}
      />
    )}
  </LineChart>
</ResponsiveContainer>
```

## Database Schema

### Companies Table
```sql
-- sec-project/backend/app/models/company.py
CREATE TABLE companies (
    id INTEGER PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    cik VARCHAR(20),
    sic_code VARCHAR(10),
    industry VARCHAR(255),
    sector VARCHAR(255),
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP
);
```

### Documents Table
```sql
-- sec-project/backend/app/models/document.py
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    accession_number VARCHAR(50) UNIQUE NOT NULL,
    form_type VARCHAR(10) NOT NULL,  -- 8-K, 10-K, 10-Q
    filing_date TIMESTAMP NOT NULL,
    period_ending TIMESTAMP,
    file_url TEXT NOT NULL,
    file_size INTEGER,
    is_processed BOOLEAN DEFAULT FALSE,
    processing_status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP
);
```

### Tables Table
```sql
-- sec-project/backend/app/models/table.py
CREATE TABLE tables (
    id INTEGER PRIMARY KEY,
    document_id INTEGER REFERENCES documents(id),
    table_group_id INTEGER REFERENCES table_groups(id),
    table_index INTEGER NOT NULL,
    table_title VARCHAR(500),
    headers TEXT,  -- JSON string of column headers
    raw_html TEXT,  -- Original HTML table
    extracted_data TEXT,  -- JSON string of table data
    num_rows INTEGER,
    num_cols INTEGER,
    content_hash VARCHAR(64),  -- MD5 hash for deduplication
    embedding_vector BYTEA,  -- Vector embedding
    similarity_score FLOAT,
    is_representative BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP
);
```

### Metric Values Table
```sql
-- sec-project/backend/app/models/metric_value.py
CREATE TABLE metric_values (
    id INTEGER PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    source_table_id INTEGER REFERENCES tables(id),
    canonical_metric_name VARCHAR(255) NOT NULL,  -- Normalized metric name
    original_label VARCHAR(512),  -- Raw label from table
    value FLOAT NOT NULL,
    unit_text VARCHAR(64),
    unit_multiplier FLOAT,
    filing_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Key Features

### 1. Deduplication
- **Tables**: Uses `content_hash` to prevent duplicate table extraction
- **Metrics**: Checks for existing metrics with same company, table, canonical name, and date
- **Companies**: Prevents duplicate ticker entries

### 2. Rate Limiting
- **Embedding API**: 0.3-0.5 second delays between calls to avoid quota limits
- **SEC API**: Built-in rate limiting in the SEC extractor service

### 3. Error Handling
- **Graceful failures**: Continues processing even if individual embeddings fail
- **Job tracking**: Detailed progress events and error logging
- **Fallback providers**: Multiple embedding services for redundancy

### 4. Semantic Similarity
- **Table grouping**: Uses embeddings to identify similar tables across filings
- **Metric normalization**: Maps variant labels to canonical names using similarity search
- **Vector storage**: Pinecone index for fast similarity queries

### 5. Real-time Progress
- **Job polling**: Frontend polls backend every 3 seconds for progress updates
- **Detailed events**: Step-by-step progress tracking with timestamps
- **Visual feedback**: Progress bars and status messages

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

## API Endpoints

### Companies
- `GET /api/companies/` - List all companies
- `POST /api/companies/` - Create new company
- `GET /api/companies/{id}` - Get company details

### Extraction
- `POST /api/extraction/start` - Start extraction job
- `GET /api/extraction/jobs/{job_id}` - Get job status
- `GET /api/extraction/{company_ticker}/documents` - Get company documents
- `GET /api/extraction/{company_ticker}/tables` - Get company tables

### Metrics
- `GET /api/metrics/{company_ticker}/list` - List available metrics
- `GET /api/metrics/{company_ticker}/{metric_name}` - Get metric time series

### Tables
- `GET /api/tables/{table_id}` - Get table details
- `GET /api/tables/groups` - List table groups

## Development

### Project Structure
```
sec-project/
├── backend/
│   ├── app/
│   │   ├── models/          # Database models
│   │   ├── routers/         # API endpoints
│   │   ├── services/        # Business logic
│   │   └── utils/           # Utility functions
│   ├── Taskfile.yml         # Task runner commands
│   └── requirements.txt     # Python dependencies
├── frontend/
│   ├── src/
│   │   └── pages/           # Next.js pages
│   └── package.json         # Node.js dependencies
└── README.md
```

### Key Files

- **Frontend**: `sec-project/frontend/src/pages/index.js` - Main UI and data visualization
- **Backend API**: `sec-project/backend/app/routers/extraction.py` - Extraction job processing
- **Embedding Service**: `sec-project/backend/app/services/embedding_service.py` - AI model integration
- **Pinecone Service**: `sec-project/backend/app/services/pinecone_service.py` - Vector database
- **Database Models**: `sec-project/backend/app/models/` - PostgreSQL schema definitions

## Troubleshooting

### Common Build Issues

1. **Python virtual environment not activated**:
   ```bash
   # Make sure you see (venv) in your terminal prompt
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Node.js dependencies not installed**:
   ```bash
   cd frontend
   npm install
   ```

3. **Database connection errors**:
   - Ensure PostgreSQL is running
   - Check DATABASE_URL in `.env` file
   - Verify database exists: `createdb sec_project`

4. **Missing API keys**:
   - Copy `.env.example` to `.env`
   - Fill in all required API keys
   - Restart the backend server

### Common Runtime Issues

1. **Embedding API Quotas**: Check Vertex AI quotas and implement rate limiting
2. **SEC API Limits**: Verify your API key has sufficient credits
3. **Pinecone Index**: Ensure index dimension matches embedding model output (3072 for Gemini)
4. **Port conflicts**: Backend uses port 8000, frontend uses port 3000

### Logs

- **Backend logs**: Displayed in console when running `task run`
- **Job errors**: Written to `job_errors.log` in backend directory
- **Frontend errors**: Check browser console for JavaScript errors

### Verification Steps

After setup, verify everything is working:

1. **Backend health check**:
   ```bash
   curl http://localhost:8000/health
   # Should return: {"status": "healthy"}
   ```

2. **Frontend accessibility**:
   - Open `http://localhost:3000` in browser
   - Should see the SEC Filing Data Extraction interface

3. **Database connection**:
   ```bash
   cd backend
   python -c "from app.database import engine; print('Database connected successfully')"
   ```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
