from lattice_json_tools import bulk_gzipped_jsonline_files_to_dfs
import pandas as pd


class DFManager:
    def __init__(self, config):
        """

        :param config: A dictionary that contains the location of each file
        :return:
        """
        self.config = config
        self.df = pd.concat(
            bulk_gzipped_jsonline_files_to_dfs(config['filenames'])
        )
        self.df = self.df.drop_duplicates()

    def _merged_unique_df_from_dfs(self, cols_to_use):
        """

        :param list cols_to_use:
        :returns: `pandas.DataFrame` --
        """
        return self.df.loc[:, cols_to_use]\
            .drop_duplicates()

    def create_msa_data_frame(self):
        """
        Will merge all the data by ad_id
        :return: Dataframe with all data merged
        """
        cols_to_use = ['rate', '_id',
                       'city', 'city_wikidata_id',
                       'state', 'state_wikidata_id', 'age']
        return self.df.loc[:, cols_to_use].\
            dropna(how='all', subset=['city', 'state']).\
            drop_duplicates()

    def create_ad_dataframe(self):
        cols_to_use = ['rate', '_id',
                       'city', 'city_wikidata_id',
                       'state', 'state_wikidata_id']
        return self.df.loc[:, cols_to_use].\
            drop_duplicates()

    def create_entity_dataframe(self, entity):
        cols_to_use = ['rate', '_id',
                       'city', 'city_wikidata_id',
                       'state', 'state_wikidata_id',
                       entity]
        return self.df.loc[:, cols_to_use].\
            dropna(subset=[entity]).\
            drop_duplicates()

# df = CreateDataFrame(config).create_data_frame()
# msa_features = MakeMSA(df).get_msa_features()
