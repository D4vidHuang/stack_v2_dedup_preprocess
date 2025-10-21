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

# --- 1. 从Hugging Face下载元数据和代码内容 ---
def fetch_python_samples(output_file="python_samples.jsonl"):
    dataset_name = "bigcode/the-stack-v2-dedup"
    subset_name = "Python"
    num_samples_to_save = 100
    
    print(f"正在从 '{dataset_name}' (子集: {subset_name}) 加载数据流...")
    
    ds = load_dataset(dataset_name, subset_name, split="train", streaming=True)
    
    saved_count = 0
    
    with open(output_file, "w", encoding="utf-8") as f_out:
        for row in ds:
            if saved_count >= num_samples_to_save:
                print(f"\n已成功保存 {num_samples_to_save} 个样本到 {output_file}。")
                break
            
            try:
                print(f"\r正在处理第 {saved_count + 1}/{num_samples_to_save} 个... "
                      f"Repo: {row['repo_name']}, Path: {row['path']}", end="")
                
                # 下载实际的文件内容
                content_data = download_contents(row["blob_id"], row["src_encoding"])
                
                # 将下载到的内容添加到原始 'row' 字典中
                row["content"] = content_data["content"]
                
                # --- 错误修复：转换 datetime ---
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
                # --- 修复结束 ---
                
                f_out.write(json.dumps(row) + "\n")
                saved_count += 1
                
            except Exception as e:
                print(f"\n跳过文件 {row['blob_id']}，原因: {e}")
                continue

    print(f"\n下载阶段完成！数据已保存到 {output_file}")


# --- 2. (新功能) 将代码提取到单独的文件 ---
def extract_code_to_files(input_file="python_samples.jsonl", output_dir="python_code_output"):
    """
    读取 .jsonl 文件，并将每行中的 'content' 字段保存为单独的 .py 文件。
    """
    print(f"\n开始从 {input_file} 提取代码到文件夹 {output_dir}...")
    
    # 1. 创建输出文件夹（如果它不存在）
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"已创建文件夹: {output_dir}")
        
    # 2. 检查输入的 jsonl 文件是否存在
    if not os.path.exists(input_file):
        print(f"错误：找不到输入文件 {input_file}。请先运行 fetch_python_samples()。")
        return

    # 3. 逐行读取 jsonl 文件
    try:
        with open(input_file, "r", encoding="utf-8") as f_in:
            count = 0
            for line in f_in:
                try:
                    # 解析 JSON 数据
                    data = json.loads(line)
                    
                    # 提取需要的信息
                    content = data.get("content")
                    blob_id = data.get("blob_id")
                    
                    if not content or not blob_id:
                        print(f"跳过一行，缺少 'content' 或 'blob_id'。")
                        continue
                        
                    # 构
                    # 建输出文件名
                    filename = f"{blob_id}.py"
                    output_path = os.path.join(output_dir, filename)
                    
                    # 4. 将代码内容写入 .py 文件
                    with open(output_path, "w", encoding="utf-8") as f_out:
                        f_out.write(content)
                        
                    count += 1
                    print(f"\r已提取 {count} 个文件...", end="")
                    
                except json.JSONDecodeError:
                    print(f"\n跳过一行无效的 JSON: {line[:50]}...")
                except Exception as e:
                    print(f"\n写入文件时出错: {e}")

            print(f"\n\n提取完成！总共 {count} 个 Python 文件已保存到 {output_dir} 文件夹。")

    except FileNotFoundError:
        print(f"错误：无法打开文件 {input_file}。")


# --- 主执行 ---
if __name__ == "__main__":
    
    # 定义文件名
    jsonl_filename = "python_samples.jsonl"
    code_output_directory = "python_code_output"
    
    # 步骤 1: 下载数据并保存为 .jsonl
    fetch_python_samples(output_file=jsonl_filename)
    
    # 步骤 2: 从 .jsonl 文件提取代码为 .py 文件
    extract_code_to_files(input_file=jsonl_filename, output_dir=code_output_directory)