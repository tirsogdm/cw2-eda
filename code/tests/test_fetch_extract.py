from pathlib import Path
import tempfile
import sys
import os
import argparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tasks.fetch import fetch_pdf
from tasks.extract import extract_text

parser = argparse.ArgumentParser(description="Test fetch and extract pipeline")
parser.add_argument("--paper-id", default="2301.00001", help="arXiv paper ID to test")
args = parser.parse_args()

PAPER_ID = args.paper_id

with tempfile.TemporaryDirectory() as tmpdir:
    print(f"Testing fetch for {PAPER_ID}...")
    pdf_path = fetch_pdf(PAPER_ID, tmpdir)
    
    if pdf_path:
        print(f"PDF downloaded to {pdf_path}")
        print(f"PDF size: {os.path.getsize(pdf_path)} bytes")
        
        print("Testing extraction...")
        text = extract_text(pdf_path)
        
        if text:
            print(f"Extracted {len(text)} characters")
            print(f"First 500 chars:\n{text[:500]}")
        else:
            print("Extraction failed")
    else:
        print("Fetch failed")