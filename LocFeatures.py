from datetime import datetime
import luigi
import numpy as np
import pandas as pd
from luigi import LocalTarget

from .helpers import mean_hourly_rate_df
from .MasterDF import MasterDF


def _calculate_grouped_col_stats(df, grouping_col, stat_col, prefix):
    """

    :param pandas.DataFrame df: Dataframe to analyze
    :param str grouping_col: Column to aggregate
    :param str stat_col: Column on which to calculate stats for grouping
    :param str prefix: Prefix to ad to the stat column
    :return:
    """
    grps = df.groupby(grouping_col)
    return grps[stat_col].agg(
        {prefix + '_count': len,
         prefix + '_mean': np.mean,
         prefix + '_std': np.std,
         prefix + '_ad_p05_msa': lambda x: np.percentile(x, q=5),
         prefix + '_ad_p10_msa': lambda x: np.percentile(x, q=10),
         prefix + '_ad_p15_msa': lambda x: np.percentile(x, q=15),
         prefix + '_ad_p20_msa': lambda x: np.percentile(x, q=20),
         prefix + '_ad_p25_msa': lambda x: np.percentile(x, q=25),
         prefix + '_ad_p30_msa': lambda x: np.percentile(x, q=30),
         prefix + '_ad_p35_msa': lambda x: np.percentile(x, q=35),
         prefix + '_ad_p40_msa': lambda x: np.percentile(x, q=40),
         prefix + '_ad_p45_msa': lambda x: np.percentile(x, q=45),
         prefix + '_ad_p50_msa': lambda x: np.percentile(x, q=50),
         prefix + '_ad_p55_msa': lambda x: np.percentile(x, q=55),
         prefix + '_ad_p60_msa': lambda x: np.percentile(x, q=60),
         prefix + '_ad_p65_msa': lambda x: np.percentile(x, q=65),
         prefix + '_ad_p70_msa': lambda x: np.percentile(x, q=70),
         prefix + '_ad_p75_msa': lambda x: np.percentile(x, q=75),
         prefix + '_ad_p80_msa': lambda x: np.percentile(x, q=80),
         prefix + '_ad_p85_msa': lambda x: np.percentile(x, q=85),
         prefix + '_ad_p90_msa': lambda x: np.percentile(x, q=90),
         prefix + '_ad_p95_msa': lambda x: np.percentile(x, q=95)}).\
        reset_index()



def _get_quarter(value):
    """

    :param datetime.datetime | datetime.date value:
    :return:
    """
    day = 1
    year = value.year
    month = value.month

    if month < 0 or month > 12:
        month = -1
    elif month < 4:
        month = 1
    elif month < 7:
        month = 4
    elif month < 10:
        month = 7
    else:
        month = 10

    return pd.Timestamp('{}-{}-{}'.format(year, month, day))


class LocFeatures(luigi.Task):
    loc_col = luigi.Parameter()

    def requires(self):
        return MasterDF()

    def run(self):
        wikidata_col = '{}_wikidata_id'.format(self.loc_col)
        cols_to_use = ['_id', 'rate', 'age',
                       self.loc_col, wikidata_col]

        df = pd.read_table(self.input().path,
                           usecols=cols_to_use).\
            dropna(how='all', subset=[self.loc_col]).\
            drop_duplicates()

        rate_df = df.dropna(subset=['rate'])
        rate_df = rate_df.\
            merge(mean_hourly_rate_df(rate_df),
                  left_on=['_id'], right_on=['_id']).\
            drop('rate', axis=1).\
            drop_duplicates()

        loc_stats_rate = _calculate_grouped_col_stats(rate_df,
                                                      wikidata_col,
                                                      'rate_per_hour',
                                                      'rate')

        age_df = df.dropna(subset=['age'])
        loc_stats_age = _calculate_grouped_col_stats(age_df,
                                                     wikidata_col,
                                                     'age',
                                                     'age')

        loc_stats_rate.merge(loc_stats_age,
                             how='outer',
                             left_on=[wikidata_col], right_on=[wikidata_col]).\
            merge(df.loc[:, [self.loc_col, wikidata_col]],
                  left_on=[wikidata_col], right_on=[wikidata_col]).\
            to_csv(self.output().path,
                   sep='\t',
                   index=False)

    def output(self):
        return LocalTarget(
            'data/output/location_characteristics_{}.tsv'.format(self.loc_col)
        )


if __name__ == '__main__':
    luigi.run(['--local-scheduler'], main_task_cls=LocFeatures)
