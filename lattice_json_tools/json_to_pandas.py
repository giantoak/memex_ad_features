def gzipped_jsonline_file_to_df(fpath):
    """

    :param str fpath: Path to GZIP file of JSON lines
    """
    import ujson as json
    from pandas import DataFrame
    import gzip
    from numpy import nan

    """cols_to_use = ['_id', 'age',
                   'email', 'flag',
                   # 'location_city_lat_lon', 'location_state_lat_lon',
                   'location_city_name', 'location_state_name',
                   'location_city_wikidata_id', 'location_state_wikidata_id',
                   'phone', 'postdatetime', 'content',
                   'rate']"""

    cols_to_use = ['_id',
                   'age',
                   'location_city_name',
                   'location_state_name',
                   'location_city_wikidata_id',
                   'location_state_wikidata_id',
                   'phone',
                   'content',
                   'rate']

    df = DataFrame.from_records((json.loads(line) for line in gzip.open(fpath)),
                                columns=cols_to_use)

    # For the moment, we're skipping ingesting lat_lons to save memory
    df = df.rename(columns={'location_city_name': 'city',
                            'location_state_name': 'state',
                            'location_city_wikidata_id': 'city_wikidata_id',
                            'location_state_wikidata_id': 'state_wikidata_id'})
                              # 'location_city_lat_lon': 'city_lat_lon',
                              # 'location_state_lat_lon': 'state_lat_lon'})

    # For the moment, we're skipping ingesting lat_lons to save memory
    # for loc_type in ['city', 'state']:
    #     col = '{}_lat_lon'.format(loc_type)
    #     df.loc[:, col] = df.loc[:, col].apply(lambda x:
    #                                           (x[0], x[1]) if isinstance(x, list)
    #                                           else (nan, nan))

    df.age = df.age.astype(float)
    df.city_wikidata_id = df.city_wikidata_id.fillna(-1).astype(int)
    df.state_wikidata_id = df.state_wikidata_id.fillna(-1).astype(int)
    df.content.fillna('',inplace=True)

    # For the moment, we are dropping all entities with no age or rate
    # to save memory
    """df = df.dropna(how='all',
                   subset=['age', 'rate'])"""

    return df.drop_duplicates()


def bulk_gzipped_jsonline_files_to_dfs(glob_or_list, nproc=20):
    """
    This returns a DataFrame of the content of glob_or_list. IT MAY CONTAIN DUPLICATES
    :param str glob_or_list:
    :param int nproc:
    :param int_merge_threshold
    :returns: `pandas.DataFrame` --
    """
    import multiprocessing as mp
    from tqdm import tqdm

    if isinstance(glob_or_list, str):
        from glob import glob
        glob_or_list = glob(glob_or_list)

    if len(glob_or_list) == 0:
        return []

    pool = mp.Pool(min(nproc, len(glob_or_list)))
    dfs = []
    for df in tqdm(pool.imap_unordered(gzipped_jsonline_file_to_df, glob_or_list),
                   total=len(glob_or_list)):
        dfs.append(df)

    pool.close()
    pool.join()

    return dfs
