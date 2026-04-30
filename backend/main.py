import logging.config
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import companies, documents, extraction, financial_tables, jobs, proxy, table_analysis
from app.database import engine, Base
from contextlib import asynccontextmanager

# Load logging configuration
logging.config.fileConfig('logging.conf', disable_existing_loggers=False)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create database tables if they don't exist (don't drop existing ones)
    Base.metadata.create_all(bind=engine)
    yield

app = FastAPI(
    title="SEC Filing Data Extraction API",
    description="API for extracting and managing SEC filing data with BigQuery integration",
    version="2.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(companies.router, prefix="/api/companies", tags=["Companies"])
# Temporarily disabled old routers with model conflicts
app.include_router(documents.router, prefix="/api/documents", tags=["Documents"])
app.include_router(extraction.router, prefix="/api/extraction", tags=["Extraction"])
app.include_router(financial_tables.router, prefix="/api/financial-tables", tags=["Financial Tables"])
app.include_router(jobs.router, prefix="/api/jobs", tags=["Jobs"])
app.include_router(proxy.router, prefix="/api/proxy", tags=["Proxy"])
app.include_router(table_analysis.router, prefix="/api/analysis", tags=["Table Analysis"])


@app.get("/")
async def root():
    return {"message": "SEC Filing Data Extraction API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/admin/clear-embeddings")
async def clear_embeddings():
    """Clear all Pinecone embeddings (for testing)"""
    try:
        from app.services import clear_pinecone_embeddings
        clear_pinecone_embeddings()
        return {"message": "Embeddings cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear embeddings: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
