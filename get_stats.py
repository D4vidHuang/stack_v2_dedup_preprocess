import argparse
from datasets import load_dataset_builder, get_dataset_infos

LANGUAGE_CONFIG = {
    "python":     "Python",
    "javascript": "JavaScript",
    "typescript": "TypeScript",
    "java":       "Java",
    "c":          "C",
    "cpp":        "C++",
    "csharp":     "C-Sharp",
    "go":         "Go",
    "ruby":       "Ruby",
    "rust":       "Rust",
    "scala":      "Scala",
}

def get_dataset_stats(languages):
    """
    Connect to Hugging Face Hub and get the number of rows for the specified language subset.
    This method uses load_dataset_builder, which only downloads metadata, and is very fast.
    """
    dataset_name = "bigcode/the-stack-v2-dedup"
    print(f"Querying dataset '{dataset_name}' statistics from Hugging Face Hub...")
    print("This will only download metadata, and should be very fast.")
    print("-" * 45)
    
    total_files = 0
    
    for lang_key in languages:
        if lang_key not in LANGUAGE_CONFIG:
            print(f"Warning: Skipping unknown language '{lang_key}'")
            continue
            
        subset_name = LANGUAGE_CONFIG[lang_key]
        
        try:
            builder = load_dataset_builder(dataset_name, subset_name)

            num_rows = None
            if builder.info and builder.info.splits and "train" in builder.info.splits:
                num_rows = builder.info.splits["train"].num_examples

            if not isinstance(num_rows, int):
                infos = get_dataset_infos(dataset_name)
                if subset_name in infos and infos[subset_name].splits and "train" in infos[subset_name].splits:
                    num_rows = infos[subset_name].splits["train"].num_examples

            if isinstance(num_rows, int):
                print(f"Language: {lang_key:<12} | Subset: {subset_name:<12} | Files: {num_rows:,.0f}")
                total_files += num_rows
            else:
                print(f"Language: {lang_key:<12} | Subset: {subset_name:<12} | Files: Unknown")

        except Exception as e:
            print(f"Failed to get statistics for '{subset_name}': {e}")
            
    print("-" * 45)
    print(f"Total files for queried languages: {total_files:,.0f}")

def main():
    parser = argparse.ArgumentParser(description="Query the number of files in the The Stack v2 dataset.")
    
    parser.add_argument(
        "languages",
        nargs="+",
        choices=list(LANGUAGE_CONFIG.keys()) + ["all"],
        help=f"Specify the languages to query (e.g. python csharp) or use 'all' to query all supported languages."
    )
    
    args = parser.parse_args()
    
    languages_to_query = []
    if "all" in args.languages:
        languages_to_query = list(LANGUAGE_CONFIG.keys())
    else:
        languages_to_query = args.languages
        
    get_dataset_stats(languages_to_query)

if __name__ == "__main__":
    main()