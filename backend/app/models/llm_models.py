from enum import Enum


class VertexAIModel(str, Enum):
    """Available models in Vertex AI"""
    # Gemini models (most recent)
    GEMINI_FLASH = "gemini-2.5-flash"
    GEMINI_PRO = "gemini-2.5-pro"
    GEMINI_EMBEDDING = "gemini-embedding-001"
    TEXT_EMBEDDING = "text-embedding-005"
