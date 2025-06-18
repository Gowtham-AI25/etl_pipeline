import pandas as pd
from genericpath import exists
import yaml
import boto3
from pathlib import Path
import httpx
import json
import  gzip
import shutil
import gc
import requests
import csv
from openalex_etl.utils.helpers import config_yml
import logging
import sys

# Setup logger
log_file_path = "C:/Users/gowth/Documents/rag_datasets/Openalex_dataprocessing/ETL_pipeline/logs/extract_logs.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_path, mode='a'),  # append mode
        logging.StreamHandler(sys.stdout)  # optional: still print to console
    ]
)


with open("C:/Users/gowth/Documents/rag_datasets/Openalex_dataprocessing/ETL_pipeline/config/config_private.yml", "r") as f:
  config_private = yaml.safe_load(f) or {}

with open("C:/Users/gowth/Documents/rag_datasets/Openalex_dataprocessing/ETL_pipeline/config/development.yml", "r") as f:
  config = yaml.safe_load(f) or {}

class S3ExtarctorBase:
  def __init__(self, source_type, config, entity = None, config_private = None):
    # which type of entity you want to extarct
    self.entity = entity
    # different configurations required for extarct
    self.config = config

    self.source_type = source_type
    self.auth_required = config['auth_required'][self.source_type]

    # paths for different folders to save files
    
    self.base_path = Path(config['directories']['base_path'])/entity
    self.gz_path = self.base_path/config['directories']['gz']
    self.json_path = self.base_path/config['directories']['json']
    self.jsonl_path = self.base_path/config['directories']['jsonl']
    self.csv_path = Path(self.config['file_paths']['csv_file_path'])
    if source_type == 'openalex_s3':
      # main manifest_file for s3_urls
      self.manifest_file_path = config['s3_urls'][entity]
    

    if self.source_type == 'private_s3':
      self.config_private =  config_private or {}
    
    self.connection = self.authentication()
    
  # To prepare the all the needed folders to save the all different types of intermediate data
  def prepare_folders(self):
    for path in [self.gz_path, self.json_path, self.jsonl_path]:
      Path(path).mkdir(parents = True, exist_ok = True)

  # This function will helps transform the openalex s3 urls to https url to get data directly from url
  def convert_s3_to_https(self,s3_url: str) -> str:
    if s3_url.startswith("s3://openalex/"):
        return s3_url.replace("s3://openalex/", "https://openalex.s3.amazonaws.com/")
    return s3_url

  def get_data(self):
      
    with open(self.config['s3_urls'][self.entity], 'r') as f:
      entries = json.load(f)

    for i, entry in enumerate(entries['entries']):
      url = entry['url']
      url = self.convert_s3_to_https(url)
      filename = f"part_{i:03}.gz"
      dest_path = self.gz_path / filename
      print(dest_path)

      if dest_path.exists():
        continue

      client =  self.connection
      response = client.get(url)
      response.raise_for_status()
        #'wb' because .gz,.zip are binary files not text
      with open(dest_path, 'wb') as f:
        for chunk in response.iter_bytes(chunk_size=8192):
          f.write(chunk)

  def authentication(self):
      """Handles all authentication and returns the connection object (boto3 or httpx)."""
      if self.source_type == 'openalex_s3':
        return httpx.Client(timeout= 60.0)
      elif self.source_type == "private_s3":
          aws_cfg = self.config_private['aws']
          session = boto3.Session(
              aws_access_key_id=aws_cfg['access_key'],
              aws_secret_access_key=aws_cfg['secret_key'],
              region_name=aws_cfg['region']
          )
          return session.client('s3')  # return boto3 client

      elif self.source_type == "api_with_token":
          token = self.config_private['api']['token']
          headers = {"Authorization": f"Bearer {token}"}
          return httpx.Client(headers=headers, timeout=60.0)  # for APIs
      else:
        raise ValueError(f"Unknown source_type: {self.source_type}")

      
         
    
  def load_private_s3_csv_files(self):
    """
    Download all .csv files from the private S3 bucket and store them in:
    ./data/raw/csv_files/
    """
    s3 = self.connection
    aws_cfg = self.config_private['aws']
    bucket = aws_cfg['bucket']
    prefix = aws_cfg.get('prefix', '')

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' not in response:
        print(f" No files found in {bucket}/{prefix}")
        return

    # Local target folder
    target_path = Path(self.config['file_paths']['aws_private_files'])
    target_path.mkdir(parents=True, exist_ok=True)

    for obj in response['Contents']:
        key = obj['Key']
        if not key.endswith(".csv"):
            continue

        filename = key.split("/")[-1]
        local_file = target_path / filename

        print(f" Downloading {key} to {local_file}")

        try:
            obj_data = s3.get_object(Bucket=bucket, Key=key)
            with open(local_file, 'wb') as f:
                f.write(obj_data['Body'].read())
            print(f" Saved: {local_file.name}")
        except Exception as e:
            print(f" Error downloading {key}: {e}")


  def extract(self):

    for gz_file in self.gz_path.glob("*.gz"):
      output_filename = self.json_path/ gz_file.with_suffix('.json').name
      if output_filename.exists():
        continue

      with gzip.open(gz_file, 'rb') as f_in:
        with open(output_filename, 'wb') as f_out:
          shutil.copyfileobj(f_in, f_out)

      print(f"Extracted: {gz_file.name} -> {output_filename.name}")

  def combine_json(self):
    jsonl_file_path = self.jsonl_path/f"{self.entity}_jsonl_file.jsonl"
    self.config = config_yml(self.config['config'], 'file_paths', {f"{self.entity}_jsonl_path": str(jsonl_file_path)})
    with open(jsonl_file_path, 'w', encoding = 'utf-8') as f_out:

      for json_file in self.json_path.glob("*.json"):
        try:
          with open(json_file, "r", encoding='utf-8') as f:
            for line in f:
              data = json.loads(line.strip())
              f_out.write(json.dumps(data) + '\n')
        except json.JSONDecodeError as e:
          print(f"skipping {json_file}: JSONDEcoderError - {e}")

        print(f"{json_file} is completed")

  def to_csv(self, chunk_size = 10000):
    # jsonl_file_path = "/content/C:/Users/gowth/Documents/rag_datasets/Openalex_dataprocessing/ETL_pipeline/data/raw/sources/jsonl_files/main_jsonl_file"
    csv_path = Path(self.csv_path /f"{self.entity}.csv")

        # 🔁 Ensure JSONL path is updated in config if missing
    jsonl_path_key = f"{self.entity}_jsonl_path"
    if jsonl_path_key not in self.config.get('file_paths', {}):
        jsonl_file_path = self.jsonl_path / f"{self.entity}_jsonl_file.jsonl"
        self.config = config_yml(
            self.config['config'],
            'file_paths',
            {jsonl_path_key: str(jsonl_file_path)}
        )
    else:
        jsonl_file_path = Path(self.config['file_paths'][jsonl_path_key])

    self.config = config_yml(
          self.config['config'],
          'file_paths',
          {f"{self.entity}_csv_file": str(csv_path)}
        )
    
    is_first_chunk = True
    chunk = []

    with open(jsonl_file_path, 'r', encoding='utf-8') as f:
      for line in f:
        try:
           record = json.loads(line.strip())
           chunk.append(record)
           if len(chunk) >= chunk_size:
             df = pd.DataFrame(chunk)
             df.to_csv(csv_path, mode = 'a', index = False, header = is_first_chunk)
             is_first_chunk = False
             chunk = []
             print("chunk completed")
        except json.JSONDecodeError as e:
          print(f"skipping invalid json line {e}")
    if chunk:
      pd.DataFrame(chunk).to_csv(csv_path,  mode = 'a', index=False, header = is_first_chunk)
  
  def clean_up_memory(self):
    """Cleans up memory by deleting large variables and forcing garbage collection."""
    attrs = list(self.__dict__.keys())
    for attr in attrs:
      delattr(self, attr)
    gc.collect()
    print(" Memory cleanup completed.")
  
  import os

  def clean_intermediate_files(self):
    """Delete all intermediate .gz, .json, and .jsonl files."""
    # Delete .gz files
    for gz_file in self.gz_path.glob("*.gz"):
        gz_file.unlink()
        print(f"Deleted: {gz_file}")

    # Delete .json files
    for json_file in self.json_path.glob("*.json"):
        json_file.unlink()
        print(f"Deleted: {json_file}")

    # Delete .jsonl file
    jsonl_file = self.jsonl_path / f"{self.entity}_jsonl_file.jsonl"
    if jsonl_file.exists():
        jsonl_file.unlink()
        print(f"Deleted: {jsonl_file}")

     
  def run(self):
      self.prepare_folders()
      self.get_data()
      self.extract()
      self.combine_json()
      self.to_csv()
      print("completedt {self.entity}")



class OpenAlexRawTopicDumper:
    def __init__(self, config_file=config):
        self.config = config_file
        self.base_url = self.config["openalex_api"]["topics_base_url"]
        self.per_page = self.config["openalex_api"]["per_page"]
        # self.output_file = self.config["api_files"]["topics_csv_path"]
        self.output_file = config['api_files']['topics_csv_path']
        self.cursor = "*"
        self.count = 0

    def fetch_and_save(self):
        with open(self.output_file, "w", newline='', encoding="utf-8") as f:
            writer = None

            while True:
                url = f"{self.base_url}?per_page={self.per_page}&cursor={self.cursor}"
                response = requests.get(url).json()
                results = response.get("results", [])

                if not results:
                    break

                for item in results:
                    if writer is None:
                        writer = csv.DictWriter(f, fieldnames=item.keys())
                        writer.writeheader()
                    writer.writerow(item)
                    self.count += 1

                print(f" Fetched {self.count} topics so far...")
                self.cursor = response.get("meta", {}).get("next_cursor")
                if not self.cursor:
                    break

        print(f"Total raw topics saved: {self.count} in file '{self.output_file}'")


class PrivateS3CSVExtractor(S3ExtarctorBase):
    def __init__(self, source_type, config, entity , config_private):
        super().__init__(source_type, config, entity, config_private)
        self.private_csv_path  = config['file_paths']['csv_file_path']

    def run(self):
      # Download all CSVs from S3
        print("📦 Starting S3 file extraction...")
        self.load_private_s3_csv_files()


class S3sources(S3ExtarctorBase):
  def __init__(self, source_type, entity):
    self.source_type_ = source_type
    self.entity_ = entity
    super().__init__(self.source_type_, config, self.entity_)

class S3institutions(S3ExtarctorBase):
  def __init__(self, source_type, entity):
    self.source_type_ = source_type
    self.entity_ = entity
    super().__init__(self.source_type_, config, self.entity_)

class S3funders(S3ExtarctorBase):
  def __init__(self, source_type, entity):
    self.source_type_ = source_type
    self.entity_ = entity
    super().__init__(self.source_type_, config, self.entity_)

class S3concepts(S3ExtarctorBase):
  def __init__(self, source_type, entity):
    self.source_type_ = source_type
    self.entity_ = entity
    super().__init__(self.source_type_, config, self.entity_)


if __name__ == "__main__":
    # Create an instance of the extractor class
    extractors = [
    S3sources("openalex_s3", "sources"),
    S3institutions("openalex_s3", "institutions"),
    S3funders("openalex_s3", "funders"),
    S3concepts("openalex_s3", "concepts")
]
    for extractor in extractors:
        print(f"\n Running ETL for: {extractor.entity}")
        extractor.run()
        print(f" completed the extraction of : {extractor.entity}")
        extractor.clean_intermediate_files()
        extractor.clean_up_memory()
        
      
    dumper = OpenAlexRawTopicDumper(config)
    dumper.fetch_and_save()

    my_s3 = PrivateS3CSVExtractor('private_s3', config=config, entity= "",config_private=config_private)
    my_s3.run()






