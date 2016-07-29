import pandas as pd
import ujson as json
import gzip


def _df_from_flattened_json(config, columns=None):
    """

    :param dict config:
    :param str | list columns: columns to use
    :returns: `pd.DataFrame` --
    """
    return pd.concat([pd.DataFrame.from_records([json.loads(line)
                                                 for line in gzip.open(filename)],
                                                columns=columns)
                      for filename in config['filenames'][0:2]])


class CreateDataFrame:
    def __init__(self, config):
        """

        :param config: A dictionary that contains the location of each file
        :return:
        """
        self.config = config
        self.df = _df_from_flattened_json(config)

    def create_msa_data_frame(self):
        """
        Will merge all the data by ad_id
        :return: Dataframe with all data merged
        """
        return self.df.loc[:, ['rate',
                               'age',
                               'location_city_name',
                               'location_state_name']]

    def create_ad_dataframe(self):
        return self.df.loc[:, ['rate',
                               '_id',
                               'location_city_name',
                               'location_state_name']]

    def create_entity_dataframe(self, entity):
        return self.df.loc[:, ['rate',
                               '_id',
                               'location_city_name',
                               'location_state_name',
                               entity]]


# df = CreateDataFrame(config).create_data_frame()
# msa_features = MakeMSA(df).get_msa_features()
