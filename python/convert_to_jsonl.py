#!/usr/bin/env python3
"""
Converts Spotify streaming history JSON arrays to newline-delimited JSON (JSONL) for BigQuery ingestion.

Usage: python3 for creating the script convert_to_jsonl.py

Input: 18 *.json files
Output: 1 JSONL file
"""

import json
import os
from pathlib import Path

def main():
    # Configuration
    input_dir = Path(".")
    output_dir = Path("./jsonl_output")
    output_dir.mkdir(exist_ok=True)

    # Find all JSON files
    json_files = list(input_dir.glob("*.json"))

    if not json_files:
        print("❌ ERROR: No .json files were found in the current directory")
        print(f"📂 Current directory: {input_dir.absolute()}")
        print("\n💡 Available files:")
        for f in input_dir.iterdir():
            print(f"   - {f.name}")
        return 1

    print(f"✅ Found {len(json_files)} JSON files\n")
    print(f"📂 Work directory: {input_dir.absolute()}")
    print(f"📁 Output directory: {output_dir.absolute()}\n")

    # Process each file
    total_records = 0
    successful_files = 0

    for json_file in json_files:
        try:
            print(f"📄 Processing: {json_file.name}")
            
            # Check file is not empty
            if json_file.stat().st_size == 0:
                print(f"⚠️ WARNING: {json_file.name} is empty, skipping...")
                continue
            
            # Read JSON file
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Verify it's an array
            if not isinstance(data, list):
                print(f"⚠️ WARNING: {json_file.name} is not an array, skipping...")
                continue
            
            if len(data) == 0:
                print(f"⚠️ WARNING: {json_file.name} has 0 registers, skipping...")
                continue
            
            # Create JSONL file
            output_file = output_dir / f"{json_file.stem}.jsonl"
            with open(output_file, 'w', encoding='utf-8') as f:
                for record in data:
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')
            
            total_records += len(data)
            successful_files += 1
            print(f"✅ Converted: {len(data):,} registers → {output_file.name}")
            
        except json.JSONDecodeError as e:
            print(f"❌ ERROR: {json_file.name} - JSON invalid in line {e.lineno}")
            print(f"Detail: {e.msg}")
        except Exception as e:
            print(f"❌ ERROR: {json_file.name} - {type(e).__name__}: {e}")

    # Summary
    print(f"\n{'='*60}")
    print(f"🎉 Conversion completed!")
    print(f"Files successfully processed: {successful_files}/{len(json_files)}")
    print(f"Total of converted registers: {total_records:,}")
    print(f"Location: {output_dir.absolute()}")
    print(f"{'='*60}")
    
    return 0

if __name__ == "__main__":
    exit(main())
