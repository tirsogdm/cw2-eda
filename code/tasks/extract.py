import requests
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET
from config import GROBID_HOST, GROBID_PORT

# TEI XML namespace
TEI_NS = {"tei": "http://www.tei-c.org/ns/1.0"}

def extract_text(pdf_path: str) -> Optional[str]:
    """
    Extract body text from a PDF using local GROBID instance.

    Args:
        pdf_path: path to PDF file on local disk

    Returns:
        extracted plain text, or None if failed
    """
    url = f"http://{GROBID_HOST}:{GROBID_PORT}/api/processFulltextDocument"

    try:
        with open(pdf_path, "rb") as f:
            response = requests.post(
                url,
                files={"input": f},
                data={
                    "consolidateHeader": 0,
                    "consolidateCitations": 0,
                    "includeRawAffiliations": 0,
                },
                timeout=120
            )
        response.raise_for_status()

        text = _parse_tei(response.text)

        if not _is_valid_text(text):
            print(f"Extracted text appears invalid for {pdf_path}")
            return None
            
        return text

    except requests.exceptions.RequestException as e:
        print(f"GROBID request failed for {pdf_path}: {e}")
        return None

    except ET.ParseError as e:
        print(f"TEI XML parsing failed for {pdf_path}: {e}")
        return None

    finally:
        # Delete PDF after extraction regardless of success
        Path(pdf_path).unlink(missing_ok=True)


def _parse_tei(tei_xml: str) -> str:
    """
    Parse TEI XML response from GROBID and extract body text.

    Args:
        tei_xml: TEI XML string from GROBID

    Returns:
        plain text body of the paper
    """
    root = ET.fromstring(tei_xml)

    # Extract all text from the body element
    body = root.find(".//tei:body", TEI_NS)
    if body is None:
        return ""

    # Walk all text nodes, join with spaces
    texts = []
    for elem in body.iter():
        if elem.text:
            texts.append(elem.text.strip())
        if elem.tail:
            texts.append(elem.tail.strip())

    return " ".join(t for t in texts if t)


def _is_valid_text(text: str, min_length: int = 100, max_garbage_ratio: float = 0.1) -> bool:
    """
    Check if extracted text is valid and readable.
    
    Args:
        text: extracted text to validate
        min_length: minimum character count to be considered valid
        max_garbage_ratio: maximum ratio of non-ASCII characters
        
    Returns:
        True if text appears valid
    """
    if len(text) < min_length:
        return False
    
    non_ascii = sum(1 for c in text if ord(c) > 127)
    garbage_ratio = non_ascii / len(text)
    
    return garbage_ratio <= max_garbage_ratio