from pathlib import Path
import sys
import numpy as np

from tasks.embed import generate_embedding
from config import EMBEDDING_DIM

# Sample text, no fetch/extract needed
SAMPLE_TEXT = """
Machine learning is a subset of artificial intelligence that enables systems to learn and improve from experience without being explicitly programmed. Deep learning is a subset of machine learning that uses neural networks with many layers to learn representations of data with multiple levels of abstraction. Transformer models have revolutionised natural language processing tasks including text classification, question answering, and semantic similarity.
"""

print("Testing embedding generation...")
embedding = generate_embedding(SAMPLE_TEXT)

assert embedding is not None, "Embedding should not be None"
assert embedding.shape == (EMBEDDING_DIM,), f"Expected shape ({EMBEDDING_DIM},), got {embedding.shape}"
assert embedding.dtype == np.float32, f"Expected float32, got {embedding.dtype}"

norm = np.linalg.norm(embedding)
assert abs(norm - 1.0) < 1e-5, f"Expected unit norm, got {norm}"

print(f"Embedding: {embedding}")
print(f"Shape: {embedding.shape}")
print(f"Dtype: {embedding.dtype}")
print(f"Norm: {norm:.6f}")
print("All assertions passed!")