#!/usr/bin/env python3
"""
Run surya_ocr on a PDF and display the results
Optimized for RTX 4080 Super (16GB VRAM)
"""
import argparse
import json
import os
import subprocess
from pathlib import Path


def run_surya_ocr(pdf_path: str, output_dir: str = "ocr_output", use_gpu: bool = True) -> Path:
    """
    Run surya OCR on a PDF file using CLI

    Args:
        pdf_path: Path to the PDF file
        output_dir: Directory to save results
        use_gpu: Whether to use GPU (default: True)

    Returns:
        Path to the results.json file
    """
    print(f"Running OCR on {pdf_path}...")

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Set environment variables for GPU
    env = os.environ.copy()
    if use_gpu:
        env['TORCH_DEVICE'] = 'cuda'
        env['DETECTOR_BATCH_SIZE'] = '48'
        env['RECOGNITION_BATCH_SIZE'] = '96'  # OCR batch size
        print("GPU mode enabled (RTX 4080 Super optimized)")
        print(f"  DETECTOR_BATCH_SIZE: 48 (~13.4GB VRAM)")
        print(f"  RECOGNITION_BATCH_SIZE: 96 (~14.4GB VRAM)")
    else:
        env['TORCH_DEVICE'] = 'cpu'
        env['DETECTOR_BATCH_SIZE'] = '2'
        env['RECOGNITION_BATCH_SIZE'] = '4'
        print("CPU mode")

    # Build surya_ocr command - no --langs option
    cmd = ["surya_ocr", pdf_path, "--output_dir", output_dir]

    print("Running surya_ocr...")
    subprocess.run(cmd, check=True, env=env)

    # Find results file - surya_ocr creates subdirectory named after input file
    pdf_name = Path(pdf_path).stem
    results_file = output_path / pdf_name / "results.json"

    if not results_file.exists():
        raise FileNotFoundError(f"Results file not found: {results_file}")

    print(f"OCR complete. Results saved to {results_file}")
    return results_file


def display_results(results_file: Path):
    """Load and display the raw JSON results"""
    with open(results_file, 'r') as f:
        results = json.load(f)

    print("\n" + "="*80)
    print("RAW RESULTS:")
    print("="*80 + "\n")

    print(json.dumps(results, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="Run surya_ocr on a PDF and display results"
    )
    parser.add_argument("pdf_file", type=str, help="Path to the PDF file")
    parser.add_argument("--output-dir", type=str, default="ocr_output", 
                       help="Directory to save OCR results (default: ocr_output)")
    parser.add_argument("--skip-ocr", action="store_true",
                       help="Skip OCR step and use existing results.json")
    parser.add_argument("--cpu", action="store_true",
                       help="Force CPU usage instead of GPU")

    args = parser.parse_args()

    pdf_path = Path(args.pdf_file)
    if not pdf_path.exists():
        print(f"Error: PDF file not found: {pdf_path}")
        return 1

    if args.skip_ocr:
        pdf_name = pdf_path.stem
        results_file = Path(args.output_dir) / pdf_name / "results.json"
        print(f"Skipping OCR, loading existing results from {results_file}")
    else:
        try:
            results_file = run_surya_ocr(
                str(pdf_path), 
                args.output_dir, 
                use_gpu=not args.cpu
            )
        except Exception as e:
            print(f"\nError during OCR: {e}")
            import traceback
            traceback.print_exc()
            return 1

    try:
        display_results(results_file)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
