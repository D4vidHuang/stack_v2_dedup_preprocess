import argparse
import json
import os
from pathlib import Path
from typing import Dict, Optional

from datasets import load_dataset


# Mapping of language keys to directory names in the dataset (data/<folder>/*)
LANG_TO_FOLDER: Dict[str, str] = {
    "python": "Python_Files",
    "javascript": "JavaScript_Files",
    "typescript": "TypeScript_Files",
    "java": "Java_Files",
    "c": "C_Files",
    "cpp": "CPP_Files",
    "csharp": "C-Sharp_Files",
    "go": "Go_Files",
    "ruby": "Ruby_Files",
    "rust": "Rust_Files",
    "scala": "Scala_Files",
}

# Mapping of file extensions for exporting code files
EXT_MAP: Dict[str, str] = {
    "python": ".py",
    "javascript": ".js",
    "typescript": ".ts",
    "java": ".java",
    "c": ".c",
    "cpp": ".cpp",
    "csharp": ".cs",
    "go": ".go",
    "ruby": ".rb",
    "rust": ".rs",
    "scala": ".scala",
}

DATASET_REPO = "AISE-TUDelft/the-stack-v2"


def choose_content(row: dict) -> Optional[str]:
    # Compatible with multiple field names
    for key in ("content", "text", "code", "source", "document"):
        if key in row and isinstance(row[key], str) and row[key]:
            return row[key]
    return None


def fetch_for_language(language: str, count: int, jsonl_path: Path, out_dir: Path) -> None:
    if language not in LANG_TO_FOLDER:
        raise ValueError(f"Unsupported language: {language}. Available: {list(LANG_TO_FOLDER.keys())}")

    folder = LANG_TO_FOLDER[language]

    # Precisely select all shards in the language directory using data_files, and use streaming to avoid local large downloads
    ds = load_dataset(
        DATASET_REPO,
        data_files={"train": f"data/{folder}/*"},
        split="train",
        streaming=True,
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    saved = 0
    ext = EXT_MAP.get(language, "")
    with jsonl_path.open("w", encoding="utf-8") as f_out:
        for row in ds:
            if saved >= count:
                print(f"Saved {saved} rows -> {jsonl_path}")
                break

            content = choose_content(row)
            if not content:
                # Try to extract from possible nested fields (e.g. row["data"]["text"])
                data_field = row.get("data")
                if isinstance(data_field, dict):
                    content = choose_content(data_field)
            if not content:
                continue

            # Generate stable file name: use path/hash fields first, otherwise use count
            stem = (
                str(row.get("blob_id")
                    or row.get("sha")
                    or row.get("hash")
                    or row.get("path")
                    or row.get("id")
                    or f"sample_{saved+1}")
                .replace("/", "_")
                .replace(" ", "_")
            )
            code_path = out_dir / f"{stem}{ext}"

            # Write code file
            try:
                code_path.write_text(content, encoding="utf-8", errors="ignore")
            except Exception:
                # If encoding fails, skip this sample
                continue

            # Serialize to JSONL (try to preserve key information)
            record = {
                "id": row.get("id", stem),
                "path": row.get("path"),
                "repo_name": row.get("repo_name") or row.get("repo") or row.get("repository"),
                "language": language,
                "content": content,
            }
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")

            saved += 1
            print(f"\r{language}: {saved}/{count} saving...", end="")

    print(f"\n{language}: done, code directory: {out_dir}")


def main():
    parser = argparse.ArgumentParser(description="Fetch code samples for a specific language from AISE-TUDelft/the-stack-v2")
    parser.add_argument(
        "language",
        choices=list(LANG_TO_FOLDER.keys()),
        help=f"Language (From: {', '.join(LANG_TO_FOLDER.keys())}）",
    )
    parser.add_argument("--count", type=int, default=1000, help="Number of samples to fetch (default 1000)")
    parser.add_argument("--jsonl", type=str, help="Output JSONL file name (default <language>_aise_samples.jsonl)")
    parser.add_argument("--outdir", type=str, help="Code output directory (default <language>_aise_code_output)")

    args = parser.parse_args()

    lang = args.language
    jsonl = args.jsonl or f"{lang}_aise_samples.jsonl"
    outdir = args.outdir or f"{lang}_aise_code_output"

    fetch_for_language(lang, args.count, Path(jsonl).resolve(), Path(outdir).resolve())


if __name__ == "__main__":
    main()


