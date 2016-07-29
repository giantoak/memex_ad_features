def gzipped_jsonline_file_to_df(fpath):
    """

    :param str fpath: Path to GZIP file of JSON lines
    """
    import ujson as json
    from pandas import DataFrame as DF
    import gzip
    from numpy import nan

    cols_to_use = ['_id', 'age',
                   'email', 'flag',
                   'location_city_lat_lon', 'location_city_name',
                   'location_state_lat_lon', 'location_state_name',
                   'phone', 'postdatetime',
                   'rate']

    df = DF.from_records((json.loads(line) for line in gzip.open(fpath)),
                         columns=cols_to_use)
    for loc_type in ['city', 'state']:
        col = 'location_{}_lat_lon'.format(loc_type)
        df.loc[:, col] = df.loc[:, col].apply(lambda x:
                                              (x[0], x[1]) if isinstance(x, list)
                                              else (nan, nan))

    return df.drop_duplicates()


def bulk_gzipped_jsonline_files_to_df(glob_or_list, nproc=20):
    """
    This returns a DataFrame of the content of glob_or_list. IT MAY CONTAIN DUPLICATES
    :param str glob_or_list:
    :param int nproc:
    :returns: `pandas.DataFrame` --
    """
    import multiprocessing as mp
    from tqdm import tqdm
    import pandas as pd

    if isinstance(glob_or_list, str):
        from glob import glob
        glob_or_list = glob(glob_or_list)

    pool = mp.Pool(min(nproc, len(glob_or_list)))
    dfs = []
    for df in tqdm(pool.imap_unordered(df_from_gzipped_json_lines, glob_or_list),
                   total=len(glob_or_list)):
        dfs.append(df)

    pool.close()
    pool.join()
    return pd.concat(dfs)


class CreateDataFrame:
    def __init__(self, config):
        """

        :param config: A dictionary that contains the location of each file
        :return:
        """
        self.config = config
        self.df = bulk_gzipped_jsonline_files_to_df(config['filenames'])

    def create_msa_data_frame(self):
        """
        Will merge all the data by ad_id
        :return: Dataframe with all data merged
        """
        return self.df.loc[:, ['rate',
                               'age',
                               'location_city_name',
                               'location_state_name']].drop_duplicates()

    def create_ad_dataframe(self):
        return self.df.loc[:, ['rate',
                               '_id',
                               'location_city_name',
                               'location_state_name']].drop_duplicates()

    def create_entity_dataframe(self, entity):
        return self.df.loc[:, ['rate',
                               '_id',
                               'location_city_name',
                               'location_state_name',
                               entity]].drop_duplicates()

# df = CreateDataFrame(config).create_data_frame()
# msa_features = MakeMSA(df).get_msa_features()
