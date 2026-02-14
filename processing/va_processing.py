#!/usr/bin/env python3
"""
OCR PDF and process results into a structured table
"""
import argparse
import json
import subprocess
from pathlib import Path


def run_surya_ocr(pdf_path: str, output_dir: str = "ocr_output") -> Path:
    """
    Run surya OCR on a PDF file

    Args:
        pdf_path: Path to the PDF file
        output_dir: Directory to save OCR results

    Returns:
        Path to the results.json file
    """
    print(f"Running OCR on {pdf_path}...")

    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Run surya OCR
    cmd = [
        "surya_ocr",
        pdf_path,
        "--results_dir", output_dir
    ]

    subprocess.run(cmd, check=True)

    # Find the results.json file
    results_file = output_path / "results.json"
    if not results_file.exists():
        raise FileNotFoundError(f"Results file not found: {results_file}")

    print(f"OCR complete. Results saved to {results_file}")
    return results_file


def load_ocr_results(results_file: Path) -> dict:
    """Load OCR results from JSON file"""
    with open(results_file, 'r') as f:
        return json.load(f)


def process_results(results: dict):
    """
    Process OCR results and convert to structured table

    Args:
        results: Dictionary from results.json
    """
    print("\n=== Processing OCR Results ===\n")

    for filename, detections in results.items():
        print(f"Processing file: {filename}")
        print(f"Total detections: {len(detections)}\n")

        # TODO: Add your processing logic here
        # For now, let's just show the first few detections
        for i, detection in enumerate(detections[:5]):
            print(f"Detection {i + 1}:")
            print(f"  Text: {detection['text'][:50]}...")  # First 50 chars
            print(f"  BBox: {detection['bbox']}")
            print(f"  Confidence: {detection['confidence']:.2f}")
            print()


def main():
    parser = argparse.ArgumentParser(
        description="OCR a PDF and process results into a structured table"
    )
    parser.add_argument(
        "pdf_file",
        type=str,
        help="Path to the PDF file to process"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="ocr_output",
        help="Directory to save OCR results (default: ocr_output)"
    )
    parser.add_argument(
        "--skip-ocr",
        action="store_true",
        help="Skip OCR step and use existing results.json"
    )

    args = parser.parse_args()

    # Validate PDF file exists
    pdf_path = Path(args.pdf_file)
    if not pdf_path.exists():
        print(f"Error: PDF file not found: {pdf_path}")
        return 1

    # Run OCR or load existing results
    if args.skip_ocr:
        results_file = Path(args.output_dir) / "results.json"
        print(f"Skipping OCR, loading existing results from {results_file}")
    else:
        results_file = run_surya_ocr(str(pdf_path), args.output_dir)

    # Load and process results
    results = load_ocr_results(results_file)
    process_results(results)

    print("\nProcessing complete!")
    return 0


if __name__ == "__main__":
    exit(main())
