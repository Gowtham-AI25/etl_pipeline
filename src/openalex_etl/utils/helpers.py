# Shared utility functions (logging, config loading, etc.)
import yaml

def config_yml(config_path, parent_key, updates: dict ):
  with open(config_path , 'r') as f:
    config = yaml.safe_load(f) or {}

  if parent_key not in config:
    config[parent_key] = {}

  for key, value in updates.items():
    config[parent_key][key] = value

  with open(config_path, 'w') as f:
    yaml.safe_dump(config, f, sort_keys=False)

  print(f"updated {parent_key} in config")
  return config

def chunk_generator(file_path, chunksize = 10000):
  for chunk in pd.read_csv(file_path, chunksize = chunksize):
    yield chunk

