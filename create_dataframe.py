from lattice_json_tools import bulk_gzipped_jsonline_files_to_dfs


class CreateDataFrame:
    def __init__(self, config):
        """

        :param config: A dictionary that contains the location of each file
        :return:
        """
        self.config = config
        self.dfs = bulk_gzipped_jsonline_files_to_dfs(config['filenames'])

    def sliced_dfs_to_merged_df(self, cols_to_use):
        import pandas as pd
        return pd.concat(df.loc[:, cols_to_use].drop_duplicates()
                         for df in self.dfs).drop_duplicates()

    def create_msa_data_frame(self):
        """
        Will merge all the data by ad_id
        :return: Dataframe with all data merged
        """
        return self.sliced_dfs_to_df(['rate',
                                      'age',
                                      'location_city_name',
                                      'location_state_name'])

    def create_ad_dataframe(self):
        return self.sliced_dfs_to_df(['rate',
                                      '_id',
                                      'location_city_name',
                                      'location_state_name'])

    def create_entity_dataframe(self, entity):
        return self.sliced_dfs_to_df(['rate',
                                      '_id',
                                      'location_city_name',
                                      'location_state_name',
                                      entity])

# df = CreateDataFrame(config).create_data_frame()
# msa_features = MakeMSA(df).get_msa_features()
