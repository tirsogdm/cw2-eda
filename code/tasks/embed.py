import numpy as np
from typing import Optional, List
from sentence_transformers import SentenceTransformer
from config import EMBEDDING_MODEL, EMBEDDING_DIM, CHUNK_SIZE, CHUNK_OVERLAP

# Load model once at module level
_model = None

def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def _chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """
    Split text into overlapping chunks by word count.

    Args:
        text: input text
        chunk_size: number of words per chunk
        overlap: number of words to overlap between chunks

    Returns:
        list of text chunks
    """
    words = text.split()
    chunks = []
    start = 0

    while start < len(words):
        end = start + chunk_size
        chunk = " ".join(words[start:end])
        chunks.append(chunk)
        start += chunk_size - overlap

    return chunks


def generate_embedding(text: str) -> Optional[np.ndarray]:
    """
    Generate a single document embedding by chunking text,
    embedding each chunk, and averaging.

    Args:
        text: extracted paper text

    Returns:
        averaged embedding vector of shape (EMBEDDING_DIM,), or None if failed
    """
    if not text or not text.strip():
        return None

    try:
        model = _get_model()
        chunks = _chunk_text(text)

        if not chunks:
            return None

        # Embed all chunks in one batch
        chunk_embeddings = model.encode(chunks, convert_to_numpy=True)

        # Average across chunks to get single document vector
        doc_embedding = np.mean(chunk_embeddings, axis=0)

        # Normalise to unit vector for cosine similarity
        norm = np.linalg.norm(doc_embedding)
        if norm > 0:
            doc_embedding = doc_embedding / norm

        return doc_embedding.astype(np.float32)

    except Exception as e:
        print(f"Embedding generation failed: {e}")
        return None