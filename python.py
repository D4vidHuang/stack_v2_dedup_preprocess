import os
import boto3
import json
import datetime
from smart_open import open as smart_open
from datasets import load_dataset

try:
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )
    s3 = session.client("s3")
    print("AWS session created successfully")
except KeyError:
    print("Error: AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY environment variable not found")
    print("Please set AWS credentials before running this script")
    exit(1)

def download_contents(blob_id, src_encoding):
    """
    Download and decode file content from Software Heritage S3 bucket
    """
    s3_url = f"s3://softwareheritage/content/{blob_id}"
    
    with smart_open(s3_url, "rb", compression=".gz", transport_params={"client": s3}) as fin:
        content = fin.read().decode(src_encoding)
    
    return {"content": content}

def fetch_python_samples():
    dataset_name = "bigcode/the-stack-v2-dedup"
    subset_name = "Python"
    output_file = "python_samples.jsonl"
    num_samples_to_save = 100
    
    print(f"Loading data stream from '{dataset_name}' (subset: {subset_name})...")
    
    ds = load_dataset(dataset_name, subset_name, split="train", streaming=True)
    
    saved_count = 0
    
    with open(output_file, "w", encoding="utf-8") as f_out:
        for row in ds:
            if saved_count >= num_samples_to_save:
                print(f"\nSuccessfully saved {num_samples_to_save} samples.")
                break
            
            try:
                print(f"\rProcessing {saved_count + 1}/{num_samples_to_save}... "
                      f"Repo: {row['repo_name']}, Path: {row['path']}", end="")
                
                content_data = download_contents(row["blob_id"], row["src_encoding"])
                
                row["content"] = content_data["content"]
                
                timestamp_keys = [
                    "visit_date", 
                    "revision_date", 
                    "committer_date", 
                    "gha_event_created_at", 
                    "gha_created_at"
                ]
                
                for key in timestamp_keys:
                    if key in row and isinstance(row[key], datetime.datetime):
                        row[key] = row[key].isoformat()
                
                f_out.write(json.dumps(row) + "\n")
                
                saved_count += 1
                
            except Exception as e:
                print(f"\nSkipping file {row['blob_id']}, reason: {e}")
                continue

    print(f"\nAll done! Data saved to {output_file}")

if __name__ == "__main__":
    fetch_python_samples()