from lattice_json_tools import bulk_gzipped_jsonline_files_to_dfs


class DFManager:
    def __init__(self, config):
        """

        :param config: A dictionary that contains the location of each file
        :return:
        """
        self.config = config
        self.dfs = bulk_gzipped_jsonline_files_to_dfs(config['filenames'])
        for df in self.dfs:
            df.rename(columns={'location_city_name': 'city',
                               'location_city_lat_lon': 'city_lat_lon',
                               'location_state_name': 'state',
                               'location_state_lat_lon': 'state_lat_lon'},
                      inplace=True)
            df.age = df.age.astype(float)  # Cover NaNs with float; wasteful

    def _merged_unique_df_from_dfs(self, cols_to_use):
        """

        :param list cols_to_use:
        :returns: `pandas.DataFrame` --
        """
        import pandas as pd
        return pd.concat(df.loc[:, cols_to_use].drop_duplicates()
                         for df in self.dfs).drop_duplicates()

    def create_msa_data_frame(self):
        """
        Will merge all the data by ad_id
        :return: Dataframe with all data merged
        """
        return self._merged_unique_df_from_dfs(['rate',
                                                'age',
                                                'city',
                                                'state',
                                                '_id'])

    def create_ad_dataframe(self):
        return self._merged_unique_df_from_dfs(['rate',
                                                '_id',
                                                'city',
                                                'state'])

    def create_entity_dataframe(self, entity):
        return self._merged_unique_df_from_dfs(['rate',
                                                '_id',
                                                'city',
                                                'state',
                                                entity])

# df = CreateDataFrame(config).create_data_frame()
# msa_features = MakeMSA(df).get_msa_features()
