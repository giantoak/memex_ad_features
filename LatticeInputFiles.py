import luigi
from luigi import LocalTarget
import os


class LatticeInputFiles(luigi.ExternalTask):

    def output(self):
        print(os.getcwd())
        return LocalTarget(os.path.join('/', 'Users', 'pmlandwehr',
                                        'wkbnch', 'memex', 'memex_ad_features',
                                        'data', 'input', 'lattice'))
