from glob import glob
import luigi
from luigi import LocalTarget
import os
import pandas as pd

from .lattice_json_tools import bulk_gzipped_jsonline_files_to_dfs
from .LatticeInputFiles import LatticeInputFiles


class MasterDF(luigi.Task):
    """
    SimpleTask prints Hello World!.
    """
    file_limit = 2
    tsv_path = 'data/output/master_df.tsv'

    def requires(self):
        return LatticeInputFiles()
        # return [S3PathTask('s3://giantoak-memex/lattice_data_store/flat')]

    def run(self):
        fpaths = glob(os.path.join(self.input().path, '*.json.gz'))
        fpaths = fpaths[:self.file_limit]

        pd.concat(bulk_gzipped_jsonline_files_to_dfs(fpaths)).\
            drop_duplicates().\
            to_csv(self.output().path,
                   sep='\t',
                   index=False)

    def output(self):
        return LocalTarget(self.tsv_path)
        # s3_path = 's3://giantoak-memex/giantoak_econ_data/dfs/' + fname
        # return S3Target(s3_path)


if __name__ == '__main__':
    luigi.run(["--local-scheduler"], main_task_cls=MasterDF)
