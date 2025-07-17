# Module for data cleaning and transformations
from Base_transformer_utils import BaseEntityTransformer, Baseutils
import logging
from itertools import chain
from tqdm import tqdm
from utils.helpers import chunk_generator
import pandas as pd
from datetime import datetime

# === Set up logging ===
logging.basicConfig(
    filename=f"openalex_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

with open(path, 'r') as f:
   meta = f.read(f)

class Funder_transform(BaseEntityTransformer, Baseutils):

  def __init__(self, config, json_utils, entity: str, pk = "funder_id"):
    if not isinstance(entity,str):
      raise(f"{entity} must be string  type")
    self.entity = entity
    self.config = config
    self.pk = pk
    self.json_utils = json_utils
    Baseutils.__init__(self, self.entity)

  def column_transformer(self, df):
     df = self.drop_columns(df, self.json_utils['columns_metadata'])
     df = self.rename_columns(df, self.json_utils)
    # If any other column related things  will done in this functions
     return df

  def str_to_iter(self, df, mul_val_cols):
    for col in mul_val_cols:
      try:
        df[col] = df[col].apply(self.eval_str_literal)
      except Exception as e:
        logging.error(f"{col} is not presented in data : {e}")
    return df

  def split_data(self, df):
    multi_val_cols = self.json_utils['columns_metadata'][self.entity]['multi_val_cols'].copy()
    df['funder_id'] = df['funder_id'].apply(self.get_ids)
    if "funder_id" not in multi_val_cols:
      multi_val_cols.insert(0, "funder_id")

    multi_cols_df = df[multi_val_cols].copy()
    multi_cols_df = self.str_to_iter(
        df = multi_cols_df,
        mul_val_cols = multi_val_cols[1:]
        )
    single_cols_df = df.drop( columns = multi_val_cols[1:])
    return single_cols_df, multi_cols_df

  def expand_multi_values(self, multi_val_col, single_val_col, pk):

    single_copy = single_val_col.copy()
    pk = "funder_id"
    dataframes = []
    ext_id_df = self.normalize_json(multi_val_col, col="external_ids")
    single_copy = single_copy.merge(ext_id_df, right_on='openalex',left_on = pk, how = 'left' )
    titles_tbl = self.explode_column( multi_val_col[[pk, "alt_titles"]], column_to_explode='alt_titles')
    dataframes.append(titles_tbl)
    multi_copy = multi_val_col.drop(columns=['alt_titles']).copy()
    multi_col = ['summary_stats', 'funder_roles', 'counts_by_year', 'associated_concepts']
    for col in multi_col:
        if col in multi_copy.columns:
            df = self.inject_id_into_nested_column(multi_copy, nested_col=col, id_col=pk)
            if col in ["summary_stats"]:  # Do not explode single dict columns
              norm = self.normalize_json(df, col=col)
            else:
              exploded = self.explode_column(df, column_to_explode=col)
              norm = self.normalize_json(exploded, col=col)

            if col == "summary_stats":
                print("single_copy.columns:", single_copy.columns)
                print("norm.columns:", norm.columns)
                single_copy = single_copy.merge(norm, on=pk, how='left')
            elif col == "funder_roles":
                funder_rows = norm[norm['role'] == 'funder'].copy()
                extra_roles = norm[norm['role'] != 'funder'].copy()
                # Only merge unique rows per `pk` if needed
                single_copy = single_copy.merge(funder_rows, on=pk, how='left')
                dataframes.append(extra_roles.reset_index(drop=True))
            else:
                dataframes.append(norm)
    return single_copy, dataframes

  def handle_null(self, single_df, dataframes:list):
    single_null_str = self.json_utils['fillna_map'][self.entity]['main_funder']
    single_df = self.fill_null_values(single_df, single_null_str)
    return single_df, dataframes

  def post_processing(self, single_df, data_frames):
    single_df = single_df.drop(columns=["wikidata", "2yr_mean_citedness", "works_count_y",  'id', 'works_count', "openalex","cited_by_count", '2yr_i10_index', '2yr_h_index'])
    single_df.rename(columns = {"works_count_x":"works_count"}, inplace = True)
    def clean_dataframes(dataframes):
      def alt_titles_(df):
        df.dropna(inplace = True)
        df.reset_index(drop = True, inplace = True)
        return df
      def extra_roles_(df):
        df = df.dropna(subset = ['funder_id', 'id']).drop_duplicates(subset = ['funder_id', 'id']).reset_index(drop = True)
        df['id'] = df['id'].apply(self.get_ids)
        df.fillna({"role":"unknown", "works_count": 0}, inplace = True)
        return df
      def counts_by_year_(df):
        df= df.dropna(subset = ['funder_id', 'year']).drop_duplicates(subset = ['funder_id', 'year']).reset_index(drop =True)
        df.fillna(0, inplace = True)
        return df
      def associated_concepts_(df):
        df.drop(columns=['wikidata', 'display_name', 'level'], inplace = True)
        df = df.dropna(subset = ['funder_id', 'id']).drop_duplicates(subset = ['funder_id', 'id']).reset_index(drop = True)
        df['id'] = df['id'].apply(self.get_ids)
        df.fillna(0.0, inplace = True)
        return df
      dataframes[0] = alt_titles_(dataframes[0])
      dataframes[1] = extra_roles_(dataframes[1])
      dataframes[2] = counts_by_year_(dataframes[2])
      dataframes[3] = associated_concepts_(dataframes[3])
      return dataframes
    data_frames = clean_dataframes(data_frames)
    return single_df, data_frames

  def run(self, df):
    df = self.column_transformer(df)
    single_df, multi_df = self.split_data(df)
    single_df, df_hold = self.expand_multi_values(multi_df, single_df, pk=self.pk)
    self.null_summary(single_df=single_df, data_frames=df_hold)
    single_df, multiple_df = self.handle_null(single_df, df_hold)
    self.null_summary(single_df=single_df,data_frames=multiple_df)
    single_df, multiple_df= self.post_processing(single_df, multiple_df)
    return single_df, multiple_df


class institution_transform(BaseEntityTransformer, Baseutils):

  def __init__(self, config, json_utils, entity: str, pk = "institution_id"):
    if not isinstance(entity,str):
      raise(f"{entity} must be string  type")
    self.entity = entity
    self.config = config
    self.pk = pk
    self.json_utils = json_utils
    Baseutils.__init__(self, self.entity)

  def column_transformer(self, df):
     df = self.drop_columns(df, self.json_utils['columns_metadata'])
     df = self.rename_columns(df, self.json_utils)
    # If any other column related things  will done in this functions
     return df

  def split_data(self, df):
    multi_val_cols = self.json_utils['columns_metadata'][self.entity]['multi_val_cols'].copy()
    df['institution_id'] = df['institution_id'].apply(self.get_ids)
    if self.pk not in multi_val_cols:
      multi_val_cols.insert(0, self.pk)

    multi_cols_df = df[multi_val_cols].copy()
    multi_cols_df = self.str_to_iter(
        df = multi_cols_df,
        mul_val_cols = multi_val_cols[1:]
        )
    single_cols_df = df.drop( columns = multi_val_cols[1:])
    return single_cols_df, multi_cols_df

  def expand_multi_values(self, single_df, multi_df):
    single_col_df = single_df.copy()
    multi_col_df = multi_df.copy()
    multi_col_df.drop(columns = ["is_super_system","repositories"], inplace = True)
    dataframes = []
    multi_col = [col for col in multi_col_df.columns if col!=self.pk]
    for col in multi_col:
      if col in ["lineage_ids",'acronyms','alternatives_names']:
        exploded = self.explode_column(multi_col_df[[self.pk,col]], column_to_explode=col)
        exploded.attrs["name"] = col
        dataframes.append(exploded)
        continue
      df = self.inject_id_into_nested_column(multi_col_df[[self.pk, col]], nested_col=col, id_col=self.pk)
      if col in ["summary_stats", "geo_location"]:
        norm = self.normalize_json(df, col = col)
        if col == "geo_location":
          norm.attrs['name'] = col
          dataframes.append(norm)
        else:
          single_df = self.safe_join_df(single_df, norm, on = self.pk, how = "left")
          single_df.attrs["name"] = "institutions_table"
        continue
      exploded = self.explode_column(df, column_to_explode=col)
      norm = self.normalize_json(exploded, col=col)
      norm.attrs['name'] = col
      dataframes.append(norm)
    return single_df, dataframes

  def handle_null(self, single_df, data_frames):
    fillmap = self.json_utils['fillna_map'][self.entity]['main_institution']
    single_df = single_df.fillna(fillmap)
    return single_df, data_frames

  def post_processing(self, single_df, data_frames):
    single_df.drop(columns=[ '2yr_h_index',
                            '2yr_i10_index', '2yr_cited_by_count', '2yr_works_count',
                            'cited_by_count_right', 'works_count_right'], inplace = True)
    single_df.rename(columns={"works_count_left": "works_count", "cited_by_count_left": "cited_by_count" }, inplace = True)

    def clean_dataframes(data_frames):
        def lineage_ids_(df):
          df.dropna(inplace=True)
          df = df.drop_duplicates()
          df['lineage_ids'] = df['lineage_ids'].apply(self.get_ids)
          return df

        def acronyms_(df):
          df.dropna(inplace = True)
          df = df.drop_duplicates().reset_index(drop = True)
          return df

        def alternatives_names_(df):
          df.dropna(inplace=True)
          df = df.drop_duplicates().reset_index(drop = True)
          return df

        def institution_roles_(df):
          df['id'] = df['id'].apply(self.get_ids)
          df = df.dropna(subset=['institution_id', 'role']).drop_duplicates(subset=['institution_id', 'role']).reset_index(drop = True)
          df = df.fillna({"id":"missing_id", "works_count": 0})
          return df

        def geo_location_(df):
          # df.drop(columns = ['region'], inplace=True)
          geo_data = df.drop(columns = ['institution_id'])
          cols = ['geonames_city_id', 'city', 'country_code', 'country', 'latitude', 'longitude']
          geo_data = geo_data[cols].drop_duplicates()
          geo_data = geo_data.dropna(subset = ['geonames_city_id']).reset_index(drop = True)
          geo_id = df[['institution_id', 'geonames_city_id']]
          geo_data.attrs['name'] = 'geo_data'
          geo_id.attrs['name'] = 'geo_id'
          return geo_data, geo_id

        def related_institutions_(df):
          df = df[['institution_id', 'id', 'relationship']].copy()
          df['id'] = df['id'].apply(self.get_ids)
          df = df.dropna(subset = ['institution_id', 'id']).drop_duplicates(subset = ['institution_id', 'id']).reset_index(drop = True)
          df.fillna("unknown", inplace=True)
          return df

        def counts_by_year_(df):
          df = df.dropna(subset = ['institution_id',	'year']).drop_duplicates(subset = ['institution_id',	'year']).reset_index(drop = True)
          df.fillna(0, inplace = True)
          return df

        def associated_concepts_(df):
          df['id'] = df['id'].apply(self.get_ids)
          df = df.drop(columns = ['wikidata', 'display_name', 'level']).dropna(subset = ['institution_id', 'id'])
          df = df.drop_duplicates(subset = ['institution_id', 'id']).reset_index(drop = True)
          df.rename(columns = {"id":"concept_id"}, inplace=True)
          df.fillna(0.0, inplace = True)
          return df

        def associated_topics_(df):
          df['id'] = df['id'].apply(self.get_ids)
          df = df[['institution_id', 'id', 'count']].dropna(subset = ['institution_id', 'id'])
          df = df.drop_duplicates(subset = ['institution_id', 'id']).reset_index(drop = True)
          df.rename(columns = {"id":"ass_topic_id"}, inplace=True)
          df.fillna(0.0, inplace = True)
          return df

        def topic_share_(df):
          df['id'] = df['id'].apply(self.get_ids)
          df = df[['institution_id', 'id', 'value']].dropna(subset = ['institution_id', 'id'])
          df = df.drop_duplicates(subset = ['institution_id', 'id']).reset_index(drop = True)
          df.rename(columns = {"id":"topic_id"}, inplace=True)
          df.fillna(0.0, inplace = True)
          return df

        data_frames[0] = lineage_ids_(data_frames[0])
        data_frames[1] = acronyms_(data_frames[1])
        data_frames[2] = alternatives_names_(data_frames[2])
        data_frames[3] = institution_roles_(data_frames[3])
        data_frames[4], extra = geo_location_(data_frames[4])
        data_frames[5] = related_institutions_(data_frames[5])
        data_frames[6] = counts_by_year_(data_frames[6])
        data_frames[7] = associated_concepts_(data_frames[7])
        data_frames[8] = associated_topics_(data_frames[8])
        data_frames[9] = topic_share_(data_frames[9])
        data_frames.append(extra)
        return data_frames
    data_frames = clean_dataframes(data_frames)
    return single_df, data_frames

  def run(self, df ):

      df1 = self.column_transformer(df)
      single_df, multi_df   = self.split_data(df1)
      single_df, dataframes = self.expand_multi_values(single_df, multi_df)
      single_df, dataframes = self.handle_null(single_df, dataframes)
      single_df, dataframes = self.post_processing(single_df, dataframes)
      return single_df, dataframes


class source_transform(BaseEntityTransformer, Baseutils):

  def __init__(self, config, json_utils, entity: str, pk = "source_id"):
    if not isinstance(entity,str):
      raise(f"{entity} must be string  type")
    self.entity = entity
    self.config = config
    self.pk = pk
    self.json_utils = json_utils
    Baseutils.__init__(self, self.entity)

  def column_transformer(self, df):

     df = self.drop_columns(df, self.json_utils['columns_metadata'])
     df = self.rename_columns(df, self.json_utils)
    # If any other column related things  will done in this functions
     return df

  def split_data(self, df):
    multi_val_cols = self.json_utils['columns_metadata'][self.entity]['multi_val_cols'].copy()
    if self.pk not in multi_val_cols:
      multi_val_cols.insert(0, self.pk)

    multi_cols_df = df[multi_val_cols].copy()
    multi_cols_df = self.str_to_iter(
        df = multi_cols_df,
        mul_val_cols = multi_val_cols[1:]
        )
    single_cols_df = df.drop( columns = multi_val_cols[1:])
    return single_cols_df, multi_cols_df

  def expand_multi_values(self, single_df, multi_df):
    single_col_df = single_df.copy()
    multi_col_df = multi_df.copy()
    dataframes = []
    multi_col = [col for col in multi_col_df.columns if col != self.pk]
    for col in multi_col:
      if col in ["host_org_lineage_ids",'alternate_titles']:
        exploded = self.explode_column(multi_col_df[[self.pk,col]], column_to_explode=col)
        exploded.attrs["name"] = col
        dataframes.append(exploded)
        continue
      df = self.inject_id_into_nested_column(multi_col_df[[self.pk, col]], nested_col=col, id_col=self.pk)
      if col in ["summary_stats"]:
        norm = self.normalize_json(df, col = col)
        single_df = self.safe_join_df(single_df, norm, on = self.pk, how = "left")
        single_df.attrs["name"] = "source_table"
        continue
      exploded = self.explode_column(df, column_to_explode=col)
      norm = self.normalize_json(exploded, col=col)
      norm.attrs['name'] = col
      dataframes.append(norm)
    return single_df, dataframes

  def handle_null(self, single_df, data_frames):
    single_df.dropna(subset = ['source_id'], inplace = True)
    fillna_dict = self.json_utils['fillna_map']["sources"]['main_source']
    single_df.fillna(fillna_dict, inplace = True)
    return single_df, data_frames

  def post_processing(self, single_df, data_frames):
    single_df.drop(columns=[ '2yr_h_index', '2yr_i10_index', '2yr_cited_by_count', '2yr_works_count',
                            'cited_by_count_right', 'works_count_right'], inplace = True, errors='ignore')

    single_df.rename(columns={"works_count_left": "works_count", "cited_by_count_left": "cited_by_count" }, inplace = True)
    for col in ['source_id', 'host_org_id']:
      single_df[col] = single_df[col].apply(self.get_ids)

    def clean_dataframes(dataframes):

      def alt_titles_(df):
        df.dropna(inplace = True)
        df.reset_index(inplace= True)
        df['source_id'] = df['source_id'].apply(self.get_ids)
        return df

      def topic_share_(df):
        df.drop(columns = ['display_name','subfield.id', 'subfield.display_name',
                          'field.id', 'field.display_name', 'domain.id', 'domain.display_name'], inplace = True, errors='ignore')
        df.rename(columns = {"id": "topic_id"}, inplace = True)
        df.dropna(subset = ['source_id', 'topic_id'], inplace = True)
        df.reset_index(drop = True, inplace=True)
        df['value'] = df['value'].fillna(0.00)
        for col in ['source_id', 'topic_id']:
          df[col] = df[col].apply(self.get_ids)
        return df

      def counts_by_year_(df):
        df = df.dropna(subset = ['source_id', 'year']).reset_index(drop = True)
        df = df.fillna(0)
        df['source_id'] = df['source_id'].apply(self.get_ids)
        return df

      def host_org_lineage_ids_(df):
        df = df.dropna().reset_index(drop = True)
        for col in df.columns:
          df[col] = df[col].apply(self.get_ids)
        return df

      def associated_topics_(df):
        df.drop(columns = ['display_name','subfield.id', 'subfield.display_name',
                          'field.id', 'field.display_name', 'domain.id', 'domain.display_name'], inplace = True, errors='ignore')
        df = df.dropna(subset = ['source_id', "id"]).reset_index(drop = True)
        df = df.fillna(0)
        df.rename(columns = {"id": "topic_id"}, inplace = True)
        for col in ["source_id", "topic_id"]:
          df[col] = df[col].apply(self.get_ids)
        return df

      def associated_concepts_(df):
        df.drop(columns = ['wikidata', 'display_name'], inplace = True, errors='ignore')
        df = df.dropna(subset = ['source_id', "id"]).reset_index(drop = True)
        df.rename(columns = {"id":"concept_id"}, inplace=True)
        df = df.fillna(0.0)
        for col in ['source_id', 'concept_id']:
          df[col] = df[col].apply(self.get_ids)
        return df

      dataframes[0] = host_org_lineage_ids_(dataframes[0])
      dataframes[1] = alt_titles_(dataframes[1])
      dataframes[2] = counts_by_year_(dataframes[2])
      dataframes[3] = associated_concepts_(dataframes[3])
      dataframes[4] = associated_topics_(dataframes[4])
      dataframes[5] = topic_share_(dataframes[5])

      return dataframes

    data_frames = clean_dataframes(data_frames)

    return single_df, data_frames

  def run(self, df):
      df1 = self.column_transformer(df)
      single_df, multi_df   = self.split_data(df1)
      single_df, dataframes = self.expand_multi_values(single_df, multi_df)
      single_df, dataframes = self.handle_null(single_df, dataframes)
      single_df, dataframes = self.post_processing(single_df, dataframes)
      return single_df, dataframes



class concept_transform(BaseEntityTransformer, Baseutils):

  def __init__(self, config, json_utils, entity: str, pk = "concept_id"):
    if not isinstance(entity,str):
      raise(f"{entity} must be string  type")
    self.entity = entity
    self.config = config
    self.pk = pk
    self.json_utils = json_utils
    Baseutils.__init__(self, self.entity)

  def column_transformer(self, df):
     df = self.drop_columns(df, self.json_utils['columns_metadata'])
     df = self.rename_columns(df, self.json_utils)
     return df

  def split_data(self, df):
    multi_val_cols = self.json_utils['columns_metadata'][self.entity]['multi_val_cols'].copy()
    df['concept_id'] = df['concept_id'].apply(self.get_ids)
    if self.pk not in multi_val_cols:
      multi_val_cols.insert(0, self.pk)

    multi_cols_df = df[multi_val_cols].copy()
    multi_cols_df = self.str_to_iter(
        df = multi_cols_df,
        mul_val_cols = multi_val_cols[1:]
        )
    single_cols_df = df.drop( columns = multi_val_cols[1:])
    return single_cols_df, multi_cols_df

  def expand_multi_values(self,single_df, multi_df, pk):
    single_col_df = single_df.copy()
    multi_col_df = multi_df.copy()
    dataframes = []
    multi_col = [col for col in multi_col_df.columns if col!='concept_id']
    for col in multi_col:
      df = self.inject_id_into_nested_column(multi_col_df[[self.pk, col]], nested_col=col, id_col=self.pk)
      if col == "summary_stats":
        df = self.normalize_json(df, col=col)
        single_col_df = self.safe_join_df(single_col_df, df, on = self.pk, how = 'left' )
      else:
        exploded = self.explode_column(df, col)
        norm = self.normalize_json(exploded , col)
        norm.attrs['name'] = col
        dataframes.append(norm)
    return single_col_df, dataframes

  def handle_null(self,single_df):
    single_null_str = self.json_utils['fillna_map'][self.entity]['main_concepts']
    single_df= single_df.fillna(single_null_str)
    return single_df

  def post_processing(self, single_df, data_frames):
    single_df = single_df.drop(columns=['works_count_right', 'cited_by_count_right', '2yr_cited_by_count', '2yr_h_index', '2yr_i10_index', '2yr_works_count'])
    single_df.rename(columns={"works_count_left":"works_count", "cited_by_count_left":"cited_by_count"}, inplace = True)

    def ancestor_concepts_(df):
      df = df.drop(columns=['display_name', 'level','wikidata'])
      df.rename(columns = {"id":"ancestor_con_id"}, inplace=True)
      df['ancestor_con_id'] = df['ancestor_con_id'].apply(self.get_ids)
      df.dropna(inplace=True)
      return df

    def related_concepts_(df):
      df = df.drop(columns=['wikidata',	'display_name',	'level']).rename(columns = {"id":"related_concepts_id"})
      df['related_concepts_id'] = df['related_concepts_id'].apply(self.get_ids)
      df = df.dropna(subset = ['concept_id', 'related_concepts_id']).fillna(0.0)
      return df

    def counts_by_year_(df):
      df['year'] = df['year'].fillna(1900)
      return df.fillna(0)

    data_frames[0] = ancestor_concepts_(data_frames[0])
    data_frames[1] = related_concepts_(data_frames[1])
    data_frames[2] = counts_by_year_(data_frames[2])

    return single_df, data_frames


  def run(self, df):
      df = self.column_transformer(df)
      single,multi = self.split_data(df)
      single_df ,data_frames = self.expand_multi_values(single, multi, self.pk)
      single_df = self.handle_null(single_df)
      single_df,data_frames = self.post_processing(single_df, data_frames)
      return single_df, data_frames


    
class paper_metadata_transformer(Baseutils):
    
    def __init__(self, df, json_utils, config, entity, pk):
        self.df = df
        self.json_utils = json_utils
        self.config = config
        self.entity = entity
        self.pk = pk
        super().__init__(self.entity)

    def split_data(self, df):
        df = df[self.json_utils['columns_metadata']['papers']['unique_cols']]
        print(len(df.columns))
        df = df.map(self.standardize_null)
        dataframes = []
        col_meta = self.json_utils['columns_metadata']['papers']
        for key, val in col_meta.items():
          if key == "unique_cols":
            break
          try:
              sub_df = df[val].copy()
              sub_df = sub_df.map(self.standardize_null)
              sub_df.attrs['name'] = key
              dataframes.append(sub_df)
          except Exception as e:
            print(f"Exception {e}")
            continue
        return dataframes[0], dataframes[1:]

    def column_transformer(self, single_df, dataframes):
        single_df = self.rename_columns(single_df, self.json_utils, sub_dict='paper_metadata')
        try:
            for i in range(len(dataframes)):
                df = dataframes[i]
                name = df.attrs.get('name')
                if name is None:
                    raise ValueError(f"Missing 'attrs[\"name\"]' for dataframe index {i}")
                dataframes[i] = self.rename_columns(df=df, df_metadata=self.json_utils, sub_dict=name)
            return single_df, dataframes
        except Exception as e:
            logging.error(f"Error {e} occurred during column transformation")
            return single_df, dataframes

    def run(self):
        single_df, dataframes = self.split_data(self.df)
        single_df, dataframes = self.column_transformer(single_df, dataframes)
        return single_df, dataframes
    


class paper_tables_processing(Baseutils):

  def __init__(self, entity, single_df ,dataframes, json_utils):
    self.entity = entity
    self.single_df = single_df
    self.dataframes = dataframes
    # self.config = config
    self.json_utils = json_utils
    # super().__init__('papers')
  
  def __call__(self, path):
    return self.run(path)

  def final_main_table(self):
    self.single_df['paper_id'] = self.single_df['paper_id'].apply(self.get_ids)
    return self.single_df

  def citation_table(self):
    citation  = self.dataframes[0].copy().applymap(self.standardize_null)
    multi_cols = ['reference_ids', 'related_work_ids']
    single_col = [col for col in citation.columns if col not in multi_cols]
    main_citation = citation[single_col].copy()
    main_citation['paper_id'] = main_citation['paper_id'].apply(self.get_ids)
    results = []
    for col in multi_cols:
      df = citation[['paper_id', col]].copy()
      df[col] = df[col].apply(self.str_to_list)
      df = df.explode(col)
      df.attrs['name'] = col
      df.dropna(subset=[col], inplace = True)
      df.reset_index(drop = True, inplace = True)
      df['paper_id'] = df['paper_id'].apply(self.get_ids)
      df[col] = df[col].apply(self.get_ids)
      results.append(df)
    return [main_citation, results]

  def author_data_table(self):
      author = self.dataframes[2].copy().applymap(self.standardize_null)
      for col in author.columns:
        author[col] = author[col].apply(self.str_to_list)
      author = self.zipping_cols(author, list(author.columns))
      author = self.str_to_iter(author, ['author_institutions'])
      def get_id(row):
        if isinstance(row, dict) and 'id' in row:
          return row['id']
      author['author_institutions'] = author['author_institutions'].apply(get_id)
      for col in ['author_id','author_institutions']:
        author[col] = author[col].apply(self.get_ids)
      author = author.drop_duplicates(subset=['author_id']).dropna(subset = ['author_id'])
      author = author.reset_index(drop = True)
      author.attrs['name'] = 'author_data'
      return author

  def paper_author_table(self):
    paper_author = self.dataframes[1].copy()
    list_cols = [col for col in paper_author.columns if col!="paper_id"]
    for col in list_cols:
      paper_author[col] = paper_author[col].apply(self.str_to_list)
    paper_author = self.zipping_cols(paper_author, list_cols, "paper_id")
    for col in ['paper_id', 'author_id']:
      paper_author[col] = paper_author[col].apply(self.get_ids)
    paper_author = paper_author.drop_duplicates().reset_index(drop = True)  
    paper_author.attrs['name'] = 'paper_author'
    return paper_author

  def publication_table(self):
    publication = self.dataframes[3].copy()
    publication = publication.infer_objects(copy=False)
    for col in ["paper_id", "primary_source_id"]:
      publication[col] = publication[col].apply(self.get_ids)
    return publication.drop_duplicates(subset = ['paper_id'])

  def locations_table(self):
    locations = self.dataframes[5].copy()
    list_cols = [col for col in locations.columns if col!="paper_id"]
    for col in list_cols:
      locations[col] = locations[col].apply(self.str_to_list)
    locations = self.zipping_cols(locations, list_cols, "paper_id")
    locations = locations.applymap(self.standardize_null)
    for col in ["paper_id", "location_source_id"]:
      locations[col] = locations[col].apply(self.get_ids)
    locations = locations.dropna(subset = ['paper_id']).drop_duplicates(subset=['paper_id', 'location_landing_page'])
    locations.attrs['name'] = 'paper_avalibility'
    return locations

  def topics_table(self):
    topics = self.dataframes[6].copy()
    main_topic = topics[['paper_id', 'primary_topic_id', 'primary_topic_score']].copy()
    for col in ['paper_id','primary_topic_id']:
      main_topic[col] = main_topic[col].apply(self.get_ids)
    all_topics = topics [['paper_id', 'topic_id', 'topic_score']].copy()
    all_topics.attrs['name'] = 'all_topics'
    keywords = topics [['paper_id', 'keyword', 'keyword_score']].copy()
    keywords.attrs['name'] = 'keywords'
    concepts = topics [['paper_id', 'concept_id', 'concept_score']].copy()
    concepts.attrs['name'] = 'concepts'
    def expand_data(df):
      data = df.copy()
      cols = [col for col in data.columns if col!='paper_id']
      for col in cols:
        data[col] = data[col].apply(self.str_to_list)
      data = self.zipping_cols(data, cols, 'paper_id')
      data = data.drop_duplicates(subset = data.columns[:2]).reset_index(drop = True)
      for col in data.columns[:2]:
        data[col] = data[col].apply(self.get_ids) 
      data.attrs['name'] = df.attrs['name']
      return data
    dataframes = []
    for df in [all_topics, keywords, concepts]:
      df = expand_data(df)
      df = df.dropna().reset_index(drop = True)
      dataframes.append(df)
    return [main_topic, dataframes]
  
  def indexed_in_table(self):
    index = self.dataframes[7].copy()
    index['indexed_in'] = index['indexed_in'].apply(self.str_to_list)
    index = self.explode_column(index, "indexed_in")
    index['paper_id'] = index['paper_id'].apply(self.get_ids)
    return index.dropna().drop_duplicates().reset_index(drop = True)

  def open_access_table(self):
    oa = self.dataframes[4].copy()
    for col in ['paper_id', 'best_oa_source_id']:
      oa[col] = oa[col].apply(self.get_ids)
    return oa

  def collect_frames(self):
    main = self.final_main_table()
    side = []
    side.extend(self.citation_table())
    side.append(self.author_data_table())
    side.append(self.paper_author_table())
    side.append(self.publication_table())
    side.append(self.open_access_table())
    side.append(self.indexed_in_table())
    side.extend(self.topics_table())
    side.append(self.locations_table())
    dataframes = list(chain.from_iterable(
        [item if isinstance(item, list) else [item] for item in side]
    ))
    return main, dataframes

  def fill_null_data(self, main, side_tables):
    fill_map = self.json_utils['fillna_map']['papers']
    main = main.fillna(fill_map[main.attrs['name']])
    for i in range(len(side_tables)):
      try:
        side_tables[i] = side_tables[i].fillna(fill_map[side_tables[i].attrs['name']])
      except Exception as e:
        print("Exception", e)
    return main, side_tables
  
    
  def run(self, path):
    main, side = self.collect_frames()
    main, side = self.fill_null_data(main, side)
    self.save_as_csv(main, side, path)
    return main, side


class lookups(Baseutils):

  def __init__(self, json_utils, entity, df):
    self.json_utils = json_utils
    self.entity = entity
    self.df= df
    self.df['id'] = self.df['id'].apply(self.get_ids)

  def topics(self, df):
    topics = df [['id', 'display_name', 'description', 'works_count', 'cited_by_count']]
    topics = topics.drop_duplicates(subset = ['id']).dropna(subset = ['id'])
    topics.reset_index(drop = True, inplace = True)
    topics.rename(columns = {"id":"topic_id"}, inplace = True)
    topics.attrs['name'] = 'topics_data'
    return topics

  def topic_keywords(self,df):
    keywords = self.str_to_iter(df[['id', 'keywords']].copy(), ['keywords'])
    keywords = keywords.explode("keywords").reset_index(drop = True)
    keywords.rename(columns = {"id":"topic_id"}, inplace = True)
    keywords.attrs['name'] = 'topic_keywords'
    keywords.attrs['desc'] = "This csv file be using to look the key words related to particular topic means which topic have papers in which keyword"
    return keywords

  def subfield_data(self, df):
    subfield = self.str_to_iter(topic[['subfield']].copy(), ['subfield'])
    subfield = self.normalize_json(subfield, col = 'subfield')
    subfield['id'] = subfield['id'].apply(self.get_ids)
    subfield.rename(columns = {"id":"subfeild_id"}, inplace = True)
    subfield = subfield.drop_duplicates().reset_index(drop = True)
    subfield.attrs['name'] = 'subfeild_data'
    return subfield

  def field_data(self, df):
    field = self.str_to_iter(topic[['field']].copy(), ['field'])
    field = self.normalize_json(field, col = 'field')
    field['id'] = field['id'].apply(self.get_ids)
    field.rename(columns = {"id":"feild_id"}, inplace = True)
    field = field.drop_duplicates().reset_index(drop = True)
    field.attrs['name'] = 'feild_data'
    return field

  def domain_data(self, df):
    domain = self.str_to_iter(df[['domain']].copy(), ['domain'])
    domain = self.normalize_json(domain, col = 'domain')
    domain['id'] = domain['id'].apply(self.get_ids)
    domain.rename(columns = {"id":"domain_id"}, inplace = True)
    domain = domain.drop_duplicates().reset_index(drop = True)
    domain.attrs['name'] = 'domain_data'
    return domain

  def topic_siblings(self, df):
    sibling = self.str_to_iter(topic[['id','siblings']].copy(), ["siblings"])
    exploded = sibling.explode('siblings').reset_index(drop = True)
    norm = self.normalize_json(exploded, col='siblings')
    norm['id'] = norm['id'].apply(self.get_ids)
    norm['topic_id'] = exploded.dropna().reset_index(drop = True)['id'].values
    norm.drop(columns=['display_name'], inplace = True)
    norm.rename(columns = {"id":"sibling_id"}, inplace = True)
    sibling = norm[['topic_id', 'sibling_id']]
    sibling.attrs['name'] = 'sibling_topics'
    return sibling

  def parent_lookup(self, df):
    level_lookup = topic[['id','subfield', 'field', 'domain']].copy()
    level_lookup = self.str_to_iter(level_lookup, mul_val_cols=['subfield', 'field', 'domain'])
    def retrive_id(row):
      return row['id'].split("/")[-1]
    level_lookup['subfield'] = level_lookup['subfield'].apply(retrive_id)
    level_lookup['field'] = level_lookup['field'].apply(retrive_id)
    level_lookup['domain'] = level_lookup['domain'].apply(retrive_id)
    level_lookup.rename(columns = {"id":"topic_id"}, inplace = True)
    level_lookup.attrs['name'] = "parent_lookup"
    return level_lookup

  def combine_df(self):
    dataframes = []
    single_df = self.topics(self.df)
    dataframes.append(self.topic_keywords(self.df))
    dataframes.append(self.subfield_data(self.df))
    dataframes.append(self.field_data(self.df))
    dataframes.append(self.domain_data(self.df))
    dataframes.append(self.topic_siblings(self.df))
    dataframes.append(self.parent_lookup(self.df))
    return single_df, dataframes
  
  def save_csv(self, single_df, dataframes, path ):
    single_df.to_csv(f"{path}lookup_main_table", index = False)
    print(f"single_df saved to path: {path}")
    for df in dataframes:
      df.to_csv(f"{path}lookup_{df.attrs['name']}", index = False)
      print(f"{path}lookup_{df.attrs['name']} saved at {path}")



def paper_data_process():
    logging.info("Started processing Papers")
    try:
        csv_path = "/content/drive/MyDrive/openAlex/Final_dedup_papers.csv"
        save_path = '/content/Finance_papers_metadata/'
        df = pd.read_csv(csv_path)
        df.rename(columns={"id": "paper_id"}, inplace=True)
        config = {}
        obj = paper_metadata_transformer(df, meta, config, "papers", "paper_id")
        single, multi = obj.run()
        del df
        processor = paper_tables_processing("papers", single, multi, meta)
        main_df, side_dfs = processor(path=None)
        processor.save_as_csv(main_df, side_dfs, path=save_path)
        logging.info("Completed processing Papers")
    except Exception as e:
        logging.error(f"Papers processing failed: {e}")
        raise

def source_data_process():
    logging.info("Started processing Sources")
    try:
        csv_path = "/content/drive/MyDrive/openAlex/final_sources.csv"
        save_path = "/content/outputs/sources"
        chunksize = 10000
        config = {}
        obj = source_transform(config=config, json_utils=meta, entity="sources", pk="source_id")
        first_chunk = True
        for i, chunk_df in enumerate(chunk_generator(csv_path, chunksize=chunksize)):
            logging.info(f"Processing Sources chunk {i+1} - shape: {chunk_df.shape}")
            single_df, multi_dfs = obj.run(chunk_df)
            obj.save_as_csv(single_df, multi_dfs, path=save_path, first_chunk=first_chunk)
            first_chunk = False
        logging.info("Completed processing Sources")
    except Exception as e:
        logging.error(f"Sources processing failed: {e}")
        raise

def concept_data_process():
    logging.info("Started processing Concepts")
    try:
        csv_path = "/content/drive/MyDrive/concepts.csv"
        save_path = "/content/Concepts_tables"
        chunksize = 10000
        config = {}
        obj = concept_transform(config=config, json_utils=meta, entity="concepts", pk="concept_id")
        first_chunk = True
        for i, chunk_df in enumerate(chunk_generator(csv_path, chunksize=chunksize)):
            logging.info(f"Processing Concepts chunk {i+1} - shape: {chunk_df.shape}")
            single_df, multi_dfs = obj.run(chunk_df)
            obj.save_as_csv(single_df, multi_dfs, path=save_path, chunk_size=chunksize, first_chunk=first_chunk)
            first_chunk = False
        logging.info("Completed processing Concepts")
    except Exception as e:
        logging.error(f"Concepts processing failed: {e}")
        raise

def institution_data_process():
    logging.info("Started processing Institutions")
    try:
        csv_path = "/content/institutions_part1_40k.csv"
        save_path = "/content/Institutions_tables"
        chunksize = 28721
        config = {}
        obj = institution_transform(config=config, json_utils=meta, entity="institution", pk="institution_id")
        first_chunk = True
        for i, chunk_df in enumerate(chunk_generator(csv_path, chunksize=chunksize)):
            logging.info(f"Processing Institutions chunk {i+1} - shape: {chunk_df.shape}")
            single_df, multi_dfs = obj.run(chunk_df)
            obj.save_as_csv(single_df, multi_dfs, path=save_path, chunk_size=chunksize, first_chunk=first_chunk)
            first_chunk = False
        logging.info("Completed processing Institutions")
    except Exception as e:
        logging.error(f"Institutions processing failed: {e}")
        raise

def funder_data_process():
    logging.info("Started processing Funders")
    try:
        csv_path = "/content/drive/MyDrive/funders.csv"
        save_path = "/content/Funder_tables"
        chunksize = 20000
        config = {}
        obj = Funder_transform(config=config, json_utils=meta, entity="funders", pk="funder_id")
        first_chunk = True
        for i, chunk_df in enumerate(chunk_generator(csv_path, chunksize=chunksize)):
            logging.info(f"Processing Funders chunk {i+1} - shape: {chunk_df.shape}")
            single_df, multi_dfs = obj.run(chunk_df)
            obj.save_as_csv(single_df=single_df, dataframes=multi_dfs, path=save_path, chunk_size=chunksize, first_chunk=first_chunk)
            first_chunk = False
        logging.info("Completed processing Funders")
    except Exception as e:
        logging.error(f"Funders processing failed: {e}")
        raise

def process_topics():
    logging.info("Started processing Topics")
    try:
        topic = pd.read_csv("/content/openalex_topics_raw.csv")
        lookups_obj = lookups(meta, "lookup", topic)
        main_df, side_dfs = lookups_obj.combine_df()
        lookups_obj.save_csv(main_df, side_dfs, "/content/topics_tables/")
        logging.info("Completed processing Topics")
    except Exception as e:
        logging.error(f"Topics processing failed: {e}")
        raise

def main():
    logging.info("=== Starting Full ETL Pipeline ===")
    try:
        paper_data_process()
        source_data_process()
        concept_data_process()
        institution_data_process()
        funder_data_process()
        process_topics()
        logging.info("✅ All ETL steps completed successfully.")
    except Exception as e:
        logging.critical(f"❌ Pipeline terminated due to error: {e}")

if __name__ == "__main__":
    main()

 
