import luigi
from luigi import LocalTarget
import pandas as pd

from .AdFeaturesPerLoc import AdFeaturesPerLoc


class AdFeaturesMerged(luigi.Task):

    loc_cols = luigi.ListParameter(default=['city', 'state'])

    def requires(self):
        return [AdFeaturesPerLoc(loc_col=loc_col)
                for loc_col in self.loc_cols]

    def run(self):
        pd.\
            concat(pd.read_table(_.path) for _ in self.input()).\
            sort_values('_id').\
            to_csv(self.output().path,
                   sep='\t',
                   index=False)

    def output(self):
        return LocalTarget('data/output/ad_characteristics_all.tsv')


if __name__ == "__main__":
    luigi.run(['--local-scheduler'], main_task_cls=AdFeaturesMerged)
