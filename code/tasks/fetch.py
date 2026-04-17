import time
import requests
import tempfile
from pathlib import Path
from typing import Optional

from config import ARXIV_EXPORT_URL, PDF_DOWNLOAD_TIMEOUT, RATE_LIMIT_SLEEP

def fetch_pdf(paper_id: str, output_dir: str) -> Optional[str]:
    """
    Fetch a PDF from arXiv export server.
    
    Args:
        paper_id: arXiv paper ID e.g. "2301.00001"
        output_dir: local directory to save PDF to
        
    Returns:
        path to downloaded PDF, or None if failed
    """
    url = f"{ARXIV_EXPORT_URL}/{paper_id}"
    output_path = Path(output_dir) / f"{paper_id.replace('/', '_')}.pdf"

    # Skip if already downloaded
    if output_path.exists():
        return str(output_path)

    try:
        response = requests.get(
            url,
            timeout=PDF_DOWNLOAD_TIMEOUT,
            headers={"User-Agent": "literature-sem-search/1.0 (research project)"},
            allow_redirects=True
        )
        response.raise_for_status()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)

        return str(output_path)

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch {paper_id}: {e}")
        return None

    finally:
        time.sleep(RATE_LIMIT_SLEEP)