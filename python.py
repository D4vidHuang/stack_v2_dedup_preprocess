import os
import boto3
import json
import datetime
from smart_open import open as smart_open
from datasets import load_dataset

# --- AWS S3 客户端设置 ---
try:
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )
    s3 = session.client("s3")
    print("AWS 会话创建成功。")
except KeyError:
    print("错误：未找到 AWS_ACCESS_KEY_ID 或 AWS_SECRET_ACCESS_KEY 环境变量。")
    print("请先设置 AWS 凭证再运行此脚本。")
    exit(1)

# --- 数据集文档中提供的下载函数 ---
def download_contents(blob_id, src_encoding):
    """
    从 Software Heritage S3 存储桶下载并解码文件内容。
    """
    s3_url = f"s3://softwareheritage/content/{blob_id}"
    
    with smart_open(s3_url, "rb", compression=".gz", transport_params={"client": s3}) as fin:
        content = fin.read().decode(src_encoding)
    
    return {"content": content}

# --- 主逻辑 ---
def fetch_python_samples():
    dataset_name = "bigcode/the-stack-v2-dedup"
    subset_name = "Python"
    output_file = "python_samples.jsonl"
    num_samples_to_save = 100
    
    print(f"正在从 '{dataset_name}' (子集: {subset_name}) 加载数据流...")
    
    ds = load_dataset(dataset_name, subset_name, split="train", streaming=True)
    
    saved_count = 0
    
    with open(output_file, "w", encoding="utf-8") as f_out:
        for row in ds:
            if saved_count >= num_samples_to_save:
                print(f"\n已成功保存 {num_samples_to_save} 个样本。")
                break
            
            try:
                print(f"\r正在处理第 {saved_count + 1}/{num_samples_to_save} 个... "
                      f"Repo: {row['repo_name']}, Path: {row['path']}", end="")
                
                # 下载实际的文件内容
                content_data = download_contents(row["blob_id"], row["src_encoding"])
                
                # 将下载到的内容添加到原始 'row' 字典中
                row["content"] = content_data["content"]
                
                # --- 错误修复：转换 datetime ---
                # json.dumps 无法序列化 datetime 对象，必须先转为字符串
                timestamp_keys = [
                    "visit_date", 
                    "revision_date", 
                    "committer_date", 
                    "gha_event_created_at", 
                    "gha_created_at"
                ]
                
                for key in timestamp_keys:
                    # 检查key是否存在，以及它是否是datetime对象
                    if key in row and isinstance(row[key], datetime.datetime):
                        # 转换为 ISO 8601 格式的字符串
                        row[key] = row[key].isoformat()
                # --- 修复结束 ---
                
                # 现在可以安全地写入 JSON 了
                f_out.write(json.dumps(row) + "\n")
                
                saved_count += 1
                
            except Exception as e:
                # 如果单个文件下载或解码失败，则跳过
                print(f"\n跳过文件 {row['blob_id']}，原因: {e}")
                continue

    print(f"\n全部完成！数据已保存到 {output_file}")

if __name__ == "__main__":
    fetch_python_samples()