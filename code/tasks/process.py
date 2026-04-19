from prefect import task, get_run_logger
from typing import Optional
import tempfile

from tasks.fetch import fetch_pdf
from tasks.extract import extract_text
from tasks.embed import generate_embedding
from tasks.save import save_embedding

@task(retries=2, retry_delay_seconds=30)
def process_paper(paper_id: str) -> Optional[bool]:
    """
    Full per-paper pipeline: fetch -> extract -> embed -> save.
    Runs on a Dask worker.

    Args:
        paper_id: arXiv paper ID

    Returns:
        True if successful, None if any stage failed
    """
    logger = get_run_logger()

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Stage 1: Fetch
            pdf_path = fetch_pdf(paper_id, tmpdir)
            if pdf_path is None:
                logger.warning(f"Fetch failed for {paper_id}, skipping")
                return None

            # Stage 2: Extract
            text = extract_text(pdf_path)
            if text is None:
                logger.warning(f"Extraction failed for {paper_id}, skipping")
                return None

        # Stage 3: Embed
        embedding = generate_embedding(text)
        if embedding is None:
            logger.warning(f"Embedding failed for {paper_id}, skipping")
            return None

        # Stage 4: Save
        success = save_embedding(paper_id, embedding)
        if not success:
            logger.warning(f"Save failed for {paper_id}, skipping")
            return None

        logger.info(f"Successfully processed {paper_id}")
        return True
    
    except Exception as e:
        logger.error(f"Unexpected error processing {paper_id}: {e}", exc_info=True)
        return None