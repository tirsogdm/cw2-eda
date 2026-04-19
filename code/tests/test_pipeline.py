from pathlib import Path
import numpy as np
import tempfile
import argparse
import faiss
import json
import sys

from tasks.fetch import fetch_pdf
from tasks.index import build_index
from tasks.save import save_embedding
from tasks.extract import extract_text
from tasks.embed import generate_embedding
from config import EMBEDDING_DIM, FAISS_INDEX_PATH, METADATA_PATH

parser = argparse.ArgumentParser(description="Test full pipeline end to end")
parser.add_argument("--paper-id", default="2301.00001", help="arXiv paper ID to test")
args = parser.parse_args()

PAPER_ID = args.paper_id

print(f"\n--- Stage 1: Fetch ---")
with tempfile.TemporaryDirectory() as tmpdir:
    pdf_path = fetch_pdf(PAPER_ID, tmpdir)
    assert pdf_path is not None, "Fetch failed"
    print(f"PDF downloaded: {pdf_path}")

    print(f"\n--- Stage 2: Extract ---")
    text = extract_text(pdf_path)
    assert text is not None, "Extraction failed"
    assert len(text) > 100, "Extracted text too short"
    print(f"Extracted {len(text)} characters")
    print(f"First 500 chars:\n{text[:500]}")

print(f"\n--- Stage 3: Embed ---")
embedding = generate_embedding(text)
assert embedding is not None, "Embedding failed"
assert embedding.shape == (EMBEDDING_DIM,), f"Wrong shape: {embedding.shape}"
assert embedding.dtype == np.float32, f"Wrong dtype: {embedding.dtype}"
norm = np.linalg.norm(embedding)
assert abs(norm - 1.0) < 1e-5, f"Not unit norm: {norm}"
print(f"Embedding generated, shape {embedding.shape}")

print(f"\n--- Stage 4: Save ---")
success = save_embedding(PAPER_ID, embedding)
assert success, "Save to MinIO failed"
print(f"Embedding saved to MinIO")

print(f"\n--- Stage 5: Build Index ---")
success = build_index()
assert success, "Index build failed"
print(f"FAISS index built")

print(f"\n--- Stage 6: Verify Index ---")
index = faiss.read_index(FAISS_INDEX_PATH)
with open(METADATA_PATH, "r") as f:
    paper_ids = json.load(f)

assert index.ntotal > 0, "Index is empty"
assert len(paper_ids) == index.ntotal, "Metadata and index size mismatch"
print(f"Index contains {index.ntotal} vectors")
print(f"Metadata contains {len(paper_ids)} paper IDs")

print(f"\n--- Stage 7: Query Index ---")
distances, indices = index.search(embedding.reshape(1, -1), k=1)
assert indices[0][0] == 0, f"Expected index 0, got {indices[0][0]}"
assert distances[0][0] > 0.99, f"Expected similarity ~1.0, got {distances[0][0]}"
print(f"Query returned paper {paper_ids[indices[0][0]]} with score {distances[0][0]:.4f}")

print(f"\nAll pipeline stages passed!")