def gzipped_jsonline_file_to_df(fpath):
    """

    :param str fpath: Path to GZIP file of JSON lines
    """
    import ujson as json
    from pandas import DataFrame
    import gzip
    from numpy import nan

    cols_to_use = ['_id', 'age',
                   'email', 'flag',
                   'location_city_lat_lon', 'location_city_name',
                   'location_state_lat_lon', 'location_state_name',
                   'phone', 'postdatetime',
                   'rate']

    df = DataFrame.from_records((json.loads(line) for line in gzip.open(fpath)),
                                columns=cols_to_use)

    for loc_type in ['city', 'state']:
        col = 'location_{}_lat_lon'.format(loc_type)
        df.loc[:, col] = df.loc[:, col].apply(lambda x:
                                              (x[0], x[1]) if isinstance(x, list)
                                              else (nan, nan))

    df.age = df.age.astype(float)
    return df.rename(columns={'location_city_name': 'city',
                              'location_city_lat_lon': 'city_lat_lon',
                              'location_state_name': 'state',
                              'location_state_lat_lon': 'state_lat_lon'}
                     ).drop_duplicates()


def bulk_gzipped_jsonline_files_to_dfs(glob_or_list, nproc=10, merge_threshold=(5, 10)):
    """
    This returns a DataFrame of the content of glob_or_list. IT MAY CONTAIN DUPLICATES
    :param str glob_or_list:
    :param int nproc:
    :param int_merge_threshold
    :returns: `pandas.DataFrame` --
    """
    import multiprocessing as mp
    from tqdm import tqdm
    import pandas as pd

    if isinstance(glob_or_list, str):
        from glob import glob
        glob_or_list = glob(glob_or_list)

    pool = mp.Pool(min(nproc, len(glob_or_list)))
    master_dfs = []
    dfs = []
    for df in tqdm(pool.imap_unordered(gzipped_jsonline_file_to_df, glob_or_list),
                   total=len(glob_or_list)):
        dfs.append(df)
        if len(dfs) >= merge_threshold[0]:
            master_dfs.append(pd.concat(dfs).drop_duplicates())
            dfs = []
            if len(master_dfs) >= merge_threshold[1]:
                master_dfs = [pd.concat(master_dfs).drop_duplicates()]

    pool.close()
    pool.join()

    if len(dfs) > 0:
        master_dfs += dfs

    if len(master_dfs) > 1:
        return pd.concat(master_dfs).drop_duplicates()

    return master_dfs[0]
