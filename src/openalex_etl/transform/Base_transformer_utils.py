from os import renames
from tokenize import endpats
from abc import  ABC, abstractmethod
from itertools import zip_longest, chain
import numpy as np
import pandas as pd
import logging
import json
import ast
import os

class BaseEntityTransformer(ABC):
  def __init__(self):
    pass

  @abstractmethod
  def column_transformer(self):
    pass

  @abstractmethod
  def split_data(self):
    pass

  @abstractmethod
  def expand_multi_values(self):
    pass

  @abstractmethod
  def handle_null(self):
    pass

  @abstractmethod 
  def post_processing(self):
    pass

  @abstractmethod
  def run():
    pass


class Baseutils:
  def __init__(self, entity: str):
    import ast
    self.frist_chunk = True
    self.entity = entity

  def is_cols_validate(self, df_cols: list, must_cols: list):
    missing_cols = [col for col in must_cols if col not in df_cols]
    if missing_cols:
      return False, missing_cols
    return True, missing_cols

  def drop_columns(self, df ,df_metadata = None):
    try:
      cols_to_drop = df_metadata[self.entity]["delete"]
      cols_present = df.columns.intersection(cols_to_drop)
      df.drop(columns = cols_present, inplace = True)

      logging.info(f"{cols_present} are dropped from the this dataframe")
      must_cols = df_metadata[self.entity]['keep']
      is_cols_valid , missing_cols = self.is_cols_validate(df.columns, must_cols)
      if not is_cols_valid:
        logging.error(f"{missing_cols} columns are missing after drop")
        raise

    except Exception as e:
      logging.error(f"Error while dropping columns: {e}")
      raise

    return df


  def rename_columns( self, df, df_metadata, sub_dict = None):
    if self.entity == "papers":
        if sub_dict is None:
            raise ValueError("sub_dict must be provided when entity is 'papers'")
        rename_map = df_metadata['rename_map'][self.entity][sub_dict]
    else:
        rename_map = df_metadata['rename_map'][self.entity]
    if not isinstance(rename_map, dict):
      raise TypeError(f"rename_map must be a dictionary")
    if not rename_map: # checking wherther dict is empty or not
      raise TypeError(f"rename_map must be non empty dictionary")

    try:

      presented_cols = df.columns.intersection(rename_map.keys())
      missing_cols = set(rename_map.keys()) - set(df.columns)
      df.rename(columns=rename_map, inplace= True)

      if missing_cols:
        logging.warning(f"{list(missing_cols)} are not presented in the dataframe")
      logging.info(f"sucessfully renamed {list(presented_cols)} in dataframe")
      # Update the metadata
      if sub_dict:
        df_metadata['columns_metadata'][self.entity][f'{sub_dict}_rename_cols'] = df.columns.tolist()
      else:
        df_metadata['columns_metadata'][self.entity]['rename_cols'] = df.columns.tolist()

      # Save to file
      with open(f"/content/drive/MyDrive/Datasets/openalex/col_metadata.json", "w") as f: # change path
        json.dump(df_metadata, f, indent=2)


    except TypeError as te:
      logging.error(f"TypeError during columns rename")
      raise
    except Exception as e:
      logging.error(f" unexpected error durig column rename: {e}")
      raise

    return df


  def safe_join_df(self,left_df: pd.DataFrame,
                 right_df: pd.DataFrame,
                 on: str = None,
                 left_on: str = None,
                 right_on: str = None,
                 how: str = 'left',
                 suffixes = ("_left",  "_right"),
                 verbose = False):
    try:
      if not isinstance(left_df, pd.DataFrame) or not isinstance(right_df, pd.DataFrame):
        raise TypeError ("Both inputs must be pandas dataframe")

      merge_kwargs = {
            "left": left_df,
            "right": right_df,
            "how": how,
            "suffixes": suffixes
      }
      if on:
        if on in left_df.columns and on in right_df.columns:
          merge_kwargs['on'] = on
        else:
          raise ValueError(f"column {on} must exist in the both columns. ")

      elif left_on and right_on:
        if left_on in left_df.columns and right_on in right_df.columns:
          merge_kwargs['left_on'] = left_on
          merge_kwargs['right_on'] = right_on
        else:
          raise ValueError(f"{left_on} or {right_on} not found in thire respective dataframes")

      else:
        raise ValueError(f"Provide either `on` (for same-name key for both) or both `left_on` and `right_on`.")

      result = pd.merge(**merge_kwargs)


      if verbose:
        logging.info(f"Merged both dataframe sucessfully using method {how}")
        logging.info(f"data frame size: {result.shape}")
        logging.info(f" columns in merged dataframe: {list(result.columns)}")

      if left_on and right_on and left_on != right_on:
        result.drop(columns= [right_on], inplace = True)
      return result

    except Exception as e:
      logging.error(f"Merge is failed due to: {e}")
      return left_df

  def inject_id_into_nested_column(self, df: pd.DataFrame,
                                  nested_col: str,
                                  id_col: str) -> pd.DataFrame:
    """
    Injects the value from `id_col` into each dict inside `nested_col`,
    whether it's a single dict or a list of dicts.
    """
        # Column existence check
    if nested_col not in df.columns:
        raise ValueError(f"Column '{nested_col}' not found in DataFrame.")
    if id_col not in df.columns:
        raise ValueError(f"Column '{id_col}' not found in DataFrame.")

    df = df.copy()

    def inject(row):
        data = row.get(nested_col)
        row_id = row.get(id_col)

        if isinstance(data, dict):
            # Single dict -> inject directly
            return {id_col: row_id, **data}

        elif isinstance(data, list):
            # List of dicts ->  inject into each of dict
            return [{id_col: row_id, **d,} for d in data]

        return data  # leave remaining types of data

    df[nested_col] = df.apply(inject, axis=1)
    return df

  def explode_column(self, df: pd.DataFrame, column_to_explode: str) -> pd.DataFrame:
      """
      Explodes a column in a DataFrame (list-like values),
      while preserving the `id_col` (default: 'funder_id').
      """
      if column_to_explode not in df.columns:
          raise ValueError(f"Missing required columns: {column_to_explode}")

      # Drop rows where the column to explode is null
      df = df[df[column_to_explode].notna()].copy()

      # Explode the column
      exploded_df = df.explode(column_to_explode).reset_index(drop = True)

      return exploded_df

  def normalize_json(self, data_frame, col = None):
    if col:
      data = data_frame[col].copy()
    else:
      data = data
    data = data.dropna()
    norm_df  = pd.json_normalize(data)
    norm_df.reset_index(drop=True, inplace=True)
    return norm_df

  def null_summary(self, single_df, data_frames = None):
    print(f"Null summary of single_df: \n {single_df.isnull().sum()}\n")
    for df in data_frames:
      print(f"Null values summary: \n {df.isnull().sum()} \n")

  def fill_null_values(self, data_frame, null_strategy):
    try:
      if not isinstance(null_strategy, dict):
        raise TypeError(f"provided null_strategy is not in the form dictionary it is type of : {type(null_strategy)}")

      missing_cols = [col for col in null_strategy if col not in data_frame.columns]
      if missing_cols:
            logging.warning(f"some columns are not presented in DataFrame: {missing_cols}")

      df = data_frame.copy()

      for col, value in null_strategy.items():
        original_nulls = df[col].isnull().sum()
        df[col] = df[col].fillna(value)
        filled_nulls = df[col].isnull().sum()
        logging.info(f"[INFO] Column '{col}': filled {original_nulls - filled_nulls} nulls with value '{value}'.")
      return df

    except Exception as e:
      logging.error(f"Null handling strategy is failed for because of {e}")
      raise

  def eval_str_literal(self, row):
    try:
      if isinstance(row, str):
        return ast.literal_eval(row)
    except (ValueError, SyntaxError, TypeError):
      return row

  def get_ids(self, row):
    if isinstance(row, str):
      return row.split("/")[-1]
    else:
      return row

  def str_to_iter(self, df, mul_val_cols):
    for col in mul_val_cols:
      try:
        df[col] = df[col].apply(self.eval_str_literal)
      except Exception as e:
        logging.error(f"{col} is not presented in data : {e}")
    return df

  def str_to_list(self, row):
    try:
        if isinstance(row, float):
            return []
        return row.split("|")
    except Exception as e:
        print(f"Error converting row to list: {row} - \n {e}")
        return []


  def zipping_cols(self,  df, list_columns, id_column=None):
      def zip_row(row):
          try:
              lists = [row[col] if isinstance(row[col], list) else [] for col in list_columns]
              zipped = zip_longest(*lists, fillvalue=None)  # Pads shorter lists
              return [
                  ({id_column: row[id_column]} if id_column else {}) | dict(zip(list_columns, values))
                  for values in zipped
              ]
          except Exception as e:
              print(f"Skipping row due to error: {e}")
              return []

      try:
          all_rows = list(chain.from_iterable(df.apply(zip_row, axis=1)))
          return pd.DataFrame(all_rows)
      except Exception as outer_e:
          print(f"Failed to expand dataframe: {outer_e}")
          return pd.DataFrame()


  def standardize_null(self,val):
      if pd.isna(val):
          return np.nan
      if isinstance(val, str) and val.strip().lower() in {"", "none", "nan", "null", 'NaN'}:
          return np.nan
      return val



  def save_as_csv(self, single_df, dataframes, path: str, chunk_size: int = 1000, first_chunk = True):
      os.makedirs(path, exist_ok=True)

      # === Save main table in chunks ===
      if isinstance(single_df, pd.DataFrame):
          main_path = f"{path}/{self.entity}_main_table.csv"
          
          for chunk in range(0, len(single_df), chunk_size):
              single_df.iloc[chunk:chunk+chunk_size].to_csv(
                  main_path,
                  mode='w' if first_chunk else 'a',
                  index=False,
                  header=first_chunk
              )
          logging.info(f"Main table saved in chunks at {main_path}")
      else:
          logging.error(f"single_df is not a DataFrame: {type(single_df)}")
          raise TypeError("single_df must be a pandas DataFrame")

      # === Save side tables in chunks ===
      for i, df in enumerate(dataframes):
          if isinstance(df, pd.DataFrame):
              name = df.attrs.get('name', f"sub_{i}")
              side_path = f"{path}/{self.entity}_{name}_table.csv"
              for chunk in range(0, len(df), chunk_size):
                  df.iloc[chunk:chunk+chunk_size].to_csv(
                      side_path,
                      mode='w' if first_chunk else 'a',
                      index=False,
                      header=first_chunk
                  )
              logging.info(f"Side table '{name}' saved in chunks at {side_path}")
          else:
              logging.warning(f"{self.entity}_side_table[{i}] is not a DataFrame (type={type(df)})")



