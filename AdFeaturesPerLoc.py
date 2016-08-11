import luigi
from luigi import LocalTarget
import numpy as np
import pandas as pd
from tqdm import tqdm

from .helpers import mean_hourly_rate_df
from .LocFeatures import LocFeatures
from .MasterDF import MasterDF


quantiles = ['rate_ad_p{}_msa'.format(str(i).zfill(2))
             for i in range(5, 96, 5)]


def _calculate_quantile_relative_loc(x):
    """
    Gets the quantile the rate is in
    :param x: slice from pandas.DataFrame
    :returns: `int` -- Quantile of rate
    """
    try:
        return np.searchsorted(x[quantiles], x['rate_per_hour'])[0]
    except (IndexError, ValueError):
        return np.nan


class AdFeaturesPerLoc(luigi.Task):

    loc_col = luigi.Parameter()

    def requires(self):
        return MasterDF(), LocFeatures(loc_col=self.loc_col)

    def run(self):

        wikidata_col = '{}_wikidata_id'.format(self.loc_col)
        cols_to_use = ['_id', 'rate'] + [self.loc_col, wikidata_col]

        df = pd.read_table(self.input()[0].path,
                           usecols=cols_to_use).\
            dropna(subset=['rate']).\
            drop_duplicates()

        print(df.shape)

        df = df.\
            merge(mean_hourly_rate_df(df),
                  left_on=['_id'], right_on=['_id']).\
            drop('rate', axis=1).\
            drop_duplicates()

        print(df.shape)

        rel_price_col = 'relative_price_to_{}'.format(self.loc_col)
        rel_quantile_col = 'relative_quantile_to_{}'.format(self.loc_col)

        loc_df = df.dropna(subset=[self.loc_col]).\
                     loc[:, ['_id', 'rate_per_hour', wikidata_col]].\
            merge(pd.read_table(self.input()[1].path,
                                usecols=[wikidata_col] +
                                        ['rate_mean', 'rate_std'] +
                                        quantiles),
                  left_on=[wikidata_col], right_on=[wikidata_col])

        loc_df[rel_price_col] = \
            (loc_df.rate_per_hour - loc_df.rate_mean) / loc_df.rate_std

        quantile_df = loc_df.loc[:, ['_id', 'rate_per_hour'] + quantiles].\
            dropna()

        tqdm.pandas(desc=rel_quantile_col)
        quantile_df[rel_quantile_col] = quantile_df.\
            progress_apply(_calculate_quantile_relative_loc, axis=1)

        df.\
            merge(loc_df.loc[:, ['_id', rel_price_col]],
                  left_on=['_id'], right_on=['_id'], how='left').\
            merge(quantile_df.loc[:, ['_id', rel_quantile_col]],
                  left_on=['_id'], right_on=['_id'], how='left').\
            to_csv(self.output().path,
                   sep='\t',
                   index=False)

    def output(self):
        return LocalTarget(
            'data/output/ad_characteristics_per_{}.tsv'.format(self.loc_col))


if __name__ == "__main__":
    luigi.run(["--local-scheduler"], main_task_cls=AdFeaturesPerLoc)
