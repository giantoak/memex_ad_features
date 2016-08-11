import luigi
from luigi import LocalTarget
import pandas as pd

from .EntityFeaturesPerLoc import EntityFeaturesPerLoc


class EntityFeaturesMerged(luigi.Task):

    entity_col = luigi.Parameter(default='phone')
    loc_cols = luigi.ListParameter(default=['city', 'state'])

    def requires(self):
        return [EntityFeaturesPerLoc(entity_col=self.entity_col,
                                     loc_col=loc_col)
                for loc_col in self.loc_cols]

    def run(self):

        df = pd.concat(pd.read_table(_.path) for _ in self.input())
        df.to_csv(self.output().path,
                  sep='\t',
                  index=False)

    def output(self):
        return LocalTarget(
            'data/output/{}_characteristics_all.tsv'.format(self.entity_col))


if __name__ == "__main__":
    luigi.run(['--local-scheduler'], main_task_cls=EntityFeaturesMerged)
