import luigi
from luigi import LocalTarget
import numpy as np
import pandas as pd

from .helpers import mean_hourly_rate_df
from .MasterDF import MasterDF


class EntityFeaturesPerLoc(luigi.Task):

    entity_col = luigi.Parameter(default='phone')
    loc_col = luigi.Parameter()

    def requires(self):
        return MasterDF()

    def run(self):

        wikidata_col = '{}_wikidata_id'.format(self.loc_col)

        cols_to_use = ['rate', '_id',
                       self.loc_col, wikidata_col,
                       self.entity_col]

        df = pd.read_table(self.input().path,
                           usecols=cols_to_use).\
            dropna(subset=[self.entity_col]).\
            drop_duplicates()

        rate_df = df.dropna(subset=['rate'])

        rate_df = rate_df.\
            merge(mean_hourly_rate_df(rate_df),
                  left_on=['_id'], right_on=['_id']).\
            drop('rate', axis=1).\
            drop_duplicates().\
            groupby(self.entity_col)['rate_per_hour'].\
            agg({'rate_count': len,
                 'rate_mean': np.mean,
                 'rate_std': np.std,
                 'rate_median': lambda x: np.percentile(x, q=50)}).\
            reset_index(level=0)

        rate_df.columns = [self.entity_col,
                           'rate_count',
                           'rate_mean',
                           'rate_std',
                           'rate_median']

        unique_loc_col = 'unique_{}'.format(wikidata_col)
        unique_loc_df = df.loc[:, [self.entity_col, wikidata_col]].\
            dropna().\
            drop_duplicates().\
            groupby(self.entity_col).\
            count().\
            reset_index().\
            rename(columns={wikidata_col: unique_loc_col})

        df = df.merge(unique_loc_df,
                      how='left',
                      left_on=self.entity_col,
                      right_on=self.entity_col)
        del unique_loc_df

        df.loc[:, unique_loc_col] = \
            df.loc[:, unique_loc_col].fillna(0).astype(int)

        df.\
            merge(rate_df, how='outer').\
            to_csv(self.output().path,
                   sep='\t',
                   index=False)

    def output(self):
        return LocalTarget(
            'data/output/{}_characteristics_per_{}.tsv'.format(
                self.entity_col,
                self.loc_col)
        )
