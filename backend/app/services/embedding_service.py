import os
import time
from typing import Optional

import numpy as np

from dotenv import load_dotenv
from ..models.llm_models import VertexAIModel
from ..config import settings

_GENAI_AVAILABLE = False
try:
    from google import genai as google_genai
    from google.genai.types import EmbedContentConfig
    _GENAI_AVAILABLE = True
except Exception:
    _GENAI_AVAILABLE = False


try:
    # Vertex AI SDK is provided by google-cloud-aiplatform
    import vertexai
    from vertexai.language_models import TextEmbeddingModel
    _VERTEX_AVAILABLE = True
except Exception:
    _VERTEX_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    _ST_AVAILABLE = True
except Exception:
    _ST_AVAILABLE = False


class EmbeddingService:
    def __init__(self):
        """
        Embedding service that prefers Vertex AI 'text-embedding-004' (768-d),
        then local SentenceTransformers (384-d).
        """
        self.vertex_project = settings.vertex_project_id
        self.vertex_location = settings.vertex_location
        self.provider: str = "none"
        self.vertex_model: Optional[TextEmbeddingModel] = None
        self.st_model: Optional["SentenceTransformer"] = None
        
        # Rate limiting: track last API call time to avoid hitting rate limits
        self.last_api_call = 0
        self.min_call_interval = 0.5  # Minimum 0.5 seconds between API calls (adjustable per provider)

        # Try Vertex AI native SDK first
        if _VERTEX_AVAILABLE and self.vertex_project:
            try:
                # Set up authentication if service account JSON is available
                service_account_json = settings.vertex_service_account_json
                if service_account_json:
                    import json
                    import tempfile
                    # Parse the service account JSON and create temporary file
                    sa_info = json.loads(service_account_json)
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                        json.dump(sa_info, f)
                        temp_creds_file = f.name
                    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_creds_file
                
                vertexai.init(project=self.vertex_project, location=self.vertex_location)
                # Use GEMINI_EMBEDDING as primary (3072-d), TEXT_EMBEDDING as fallback (768-d)
                try:
                    self.vertex_model = TextEmbeddingModel.from_pretrained(VertexAIModel.GEMINI_EMBEDDING)
                    self.provider = "vertex"
                    print(f"Vertex AI {VertexAIModel.GEMINI_EMBEDDING} initialized (3072-d).")
                except Exception as e:
                    print(f"GEMINI_EMBEDDING failed, trying TEXT_EMBEDDING: {e}")
                    try:
                        self.vertex_model = TextEmbeddingModel.from_pretrained(VertexAIModel.TEXT_EMBEDDING)
                        self.provider = "vertex"
                        print(f"Vertex AI {VertexAIModel.TEXT_EMBEDDING} initialized (768-d).")
                    except Exception as e2:
                        print(f"TEXT_EMBEDDING also failed: {e2}")
                        self.vertex_model = None
            except Exception as e:
                print(f"Vertex AI initialization failed: {e}")
                self.vertex_model = None


        # Skip Google GenAI - use Vertex AI instead
        # if self.provider == "none" and _GENAI_AVAILABLE and os.getenv("GEMINI_API_KEY"):
        #     try:
        #         self.genai_client = google_genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        #         self.genai_model_name = os.getenv("GENAI_EMBED_MODEL", "gemini-embedding-001")
        #         self.provider = "gemini_genai"
        #         print(f"Using google-genai for embeddings with model '{self.genai_model_name}' (768-d).")
        #     except Exception as e:
        #         print(f"google-genai initialization failed: {e}")



        # Fallback to SentenceTransformers
        if self.provider == "none" and _ST_AVAILABLE:
            try:
                self.st_model = SentenceTransformer("all-MiniLM-L6-v2")  # 384-d
                self.provider = "sentencetransformers"
                print("SentenceTransformer 'all-MiniLM-L6-v2' loaded (384-d).")
            except Exception as e:
                print(f"SentenceTransformer initialization failed: {e}")
                self.st_model = None

        if self.vertex_model is None and self.st_model is None:
            print("No embedding provider available. Please configure Vertex AI or install sentence-transformers.")

    def get_embedding(self, text: str) -> list[float]:
        """
        Generate an embedding vector for the given text.
        Returns an empty list on failure.
        """
        if not text or not isinstance(text, str):
            return []

        # Rate limiting: ensure minimum interval between API calls
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call
        
        # Adjust rate limit based on provider
        if self.provider == "vertex":
            min_interval = 0.3  # Vertex AI can handle faster requests
        else:
            min_interval = self.min_call_interval
            
        if time_since_last_call < min_interval:
            sleep_time = min_interval - time_since_last_call
            time.sleep(sleep_time)
        self.last_api_call = time.time()


        # Skip Google GenAI - use Vertex AI instead
        # if self.provider == "gemini_genai" and _GENAI_AVAILABLE:
        #     try:
        #         resp = self.genai_client.models.embed_content(
        #             model=self.genai_model_name,
        #             contents=[text],
        #             config=EmbedContentConfig(output_dimensionality=768)
        #         )
        #         # API may return list of embeddings or a single object depending on version
        #         emb = getattr(resp, 'embeddings', None)
        #         if emb:
        #             values = emb[0].values
        #         else:
        #         else:
        #             values = resp.embedding.values
        #         return [float(x) for x in values]
        #     except Exception as e:
        #         print(f"google-genai embedding error: {e}")
        #         # If it's a quota error, try to fall back to other providers
        #         if "RESOURCE_EXHAUSTED" in str(e) or "429" in str(e):
        #             print("Google GenAI quota exhausted, trying fallback providers...")
        #             # Don't return empty list yet, let it try other providers
        #         else:
        #             return []

        # Prefer Vertex AI 768-d
        if self.provider == "vertex" and self.vertex_model is not None:
            try:
                embeddings = self.vertex_model.get_embeddings([text])
                values = embeddings[0].values
                # Ensure list[float]
                return [float(x) for x in values]
            except Exception as e:
                print(f"Vertex AI embedding error, falling back: {e}")

        # Fallback to MiniLM 384-d
        if self.provider == "sentencetransformers" and self.st_model is not None:
            try:
                vec = self.st_model.encode(text, convert_to_tensor=False)
                return vec.tolist() if hasattr(vec, "tolist") else [float(x) for x in vec]
            except Exception as e:
                print(f"SentenceTransformers embedding error: {e}")

        return []


# Example usage
if __name__ == '__main__':
    svc = EmbeddingService()
    vec = svc.get_embedding("This is a test of the embedding service for a financial table.")
    print(f"Provider: {svc.provider}")
    print(f"Dim: {len(vec)}")
    print(f"Preview: {vec[:5]}")
