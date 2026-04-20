import json
import numpy as np
import faiss
from pathlib import Path
from typing import Optional
from prefect import flow, get_run_logger
import io
from minio import Minio
from minio.error import S3Error
from config import (
    FAISS_INDEX_PATH,
    METADATA_PATH,
    EMBEDDING_DIM,
    MINIO_HOST,
    MINIO_PORT,
    MINIO_ROOT_USER,
    MINIO_ROOT_PASSWORD,
    MINIO_BUCKET_NAME
)
from tasks.embed import generate_embedding

def _load_index():
    """
    Load FAISS index and metadata from disk.

    Returns:
        tuple of (index, paper_ids) or (None, None) if not found
    """
    if not Path(FAISS_INDEX_PATH).exists():
        return None, None
    if not Path(METADATA_PATH).exists():
        return None, None

    index = faiss.read_index(FAISS_INDEX_PATH)
    with open(METADATA_PATH, "r") as f:
        paper_ids = json.load(f)

    return index, paper_ids

def _get_minio_client() -> Minio:
    return Minio(
        f"{MINIO_HOST}:{MINIO_PORT}",
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False
    )

def _save_results(results: list, run_id: str, query: str) -> bool:
    try:
        client = _get_minio_client()
        payload = {
            "query": query,
            "run_id": run_id,
            "results": results
        }
        data = json.dumps(payload).encode('utf-8')
        client.put_object(
            MINIO_BUCKET_NAME,
            f"results/{run_id}.json",
            io.BytesIO(data),
            len(data),
            content_type="application/json"
        )
        return True
    except S3Error as e:
        return False


@flow(name="query-flow")
def query_flow(query: str, top_k: int = 10) -> Optional[list]:
    """
    Query the FAISS index with a research question.

    Args:
        query: natural language research question
        top_k: number of results to return

    Returns:
        list of dicts with paper_id and score, or None if index not found
    """
    logger = get_run_logger()

    # Load index
    index, paper_ids = _load_index()
    if index is None:
        logger.error("FAISS index not found, run indexing_flow first")
        return None

    logger.info(f"Loaded index with {index.ntotal} papers")
    logger.info(f"Query: {query}")

    # Embed query
    query_embedding = generate_embedding(query)
    if query_embedding is None:
        logger.error("Failed to generate query embedding")
        return None

    # Search index
    distances, indices = index.search(query_embedding.reshape(1, -1), k=top_k)

    # Build results
    results = []
    for rank, (idx, score) in enumerate(zip(indices[0], distances[0])):
        if idx == -1:
            continue
        results.append({
            "rank": rank + 1,
            "paper_id": paper_ids[idx],
            "score": float(score),
            "url": f"https://arxiv.org/abs/{paper_ids[idx]}"
        })

    logger.info(f"Returning {len(results)} results")
    for r in results:
        logger.info(f"  {r['rank']}. {r['paper_id']} (score: {r['score']:.4f})")

    # Save results
    import prefect.runtime.flow_run as flow_run_ctx
    run_id = flow_run_ctx.id
    _save_results(results, run_id, query)

    logger.info(f"Results saved to MinIO: results/{run_id}.json")
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True, help="Research question")
    parser.add_argument("--top-k", type=int, default=10, help="Number of results")
    args = parser.parse_args()

    results = query_flow(query=args.query, top_k=args.top_k)

    if results:
        print(f"\nTop {len(results)} results:")
        for r in results:
            print(f"  {r['rank']}. {r['paper_id']} — score: {r['score']:.4f}")
            print(f"     {r['url']}")