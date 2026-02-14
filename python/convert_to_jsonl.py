#!/usr/bin/env python3
"""
Spotify JSON to JSONL Converter
Converts Spotify streaming history JSON arrays to newline-delimited JSON (JSONL)
for BigQuery ingestion.

Usage:
    python3 convert_to_jsonl.py

Input: *.json files in current directory
Output: JSONL files in ./jsonl_output/ directory
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
        print("❌ ERROR: No se encontraron archivos .json en el directorio actual")
        print(f"📂 Directorio actual: {input_dir.absolute()}")
        print("\n💡 Archivos disponibles:")
        for f in input_dir.iterdir():
            print(f"   - {f.name}")
        return 1

    print(f"✅ Encontrados {len(json_files)} archivos JSON\n")
    print(f"📂 Directorio de trabajo: {input_dir.absolute()}")
    print(f"📁 Output directory: {output_dir.absolute()}\n")

    # Process each file
    total_records = 0
    successful_files = 0

    for json_file in json_files:
        try:
            print(f"📄 Procesando: {json_file.name}")
            
            # Check file is not empty
            if json_file.stat().st_size == 0:
                print(f"   ⚠️  AVISO: {json_file.name} está vacío, saltando...")
                continue
            
            # Read JSON file
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Verify it's an array
            if not isinstance(data, list):
                print(f"   ⚠️  AVISO: {json_file.name} no es un array, saltando...")
                continue
            
            if len(data) == 0:
                print(f"   ⚠️  AVISO: {json_file.name} tiene 0 registros, saltando...")
                continue
            
            # Create JSONL file
            output_file = output_dir / f"{json_file.stem}.jsonl"
            with open(output_file, 'w', encoding='utf-8') as f:
                for record in data:
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')
            
            total_records += len(data)
            successful_files += 1
            print(f"   ✅ Convertido: {len(data):,} registros → {output_file.name}")
            
        except json.JSONDecodeError as e:
            print(f"   ❌ ERROR: {json_file.name} - JSON inválido en línea {e.lineno}")
            print(f"      Detalle: {e.msg}")
        except Exception as e:
            print(f"   ❌ ERROR: {json_file.name} - {type(e).__name__}: {e}")

    # Summary
    print(f"\n{'='*60}")
    print(f"🎉 Conversión completada!")
    print(f"   Archivos procesados exitosamente: {successful_files}/{len(json_files)}")
    print(f"   Total de registros convertidos: {total_records:,}")
    print(f"   Ubicación: {output_dir.absolute()}")
    print(f"{'='*60}")
    
    return 0

if __name__ == "__main__":
    exit(main())
