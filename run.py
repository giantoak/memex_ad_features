from create_dataframe import DFManager
from make_msa import MakeMSA
from make_ad import MakeAd
from make_entity import MakeEntity
from config_parser import Parser
import pandas as pd
import os
import datetime
from glob import glob

#  config = {'filenames': glob.glob('/home/ubuntu/2016_summer_camp/classifier/data/initial/lattice/*gz')}
# config = {'filenames': glob('lattice_data/flattened/flattened_before_201605/*.json.gz')[:2]}
# config = {'filenames': glob('/home/ubuntu/flat_data/*.json.gz')}
#config = {'filenames': glob('/home/gabriel/Documents/Memex/ad_features/sample_data.json.gz')}

class Run:
    def __init__(self, config):
        self.config = config
        print('Initializing DF Manager...')
        df_manager_config = {}
        df_manager_config.update(config)
        df_manager_config['file_names'] = glob(self.config['file_names'])
        self.dfm = DFManager(df_manager_config)
        print('Done!')

    def run_location_features(self, dfm):
        """
        Will get features by city and state
        :param create_dataframe.DFManager dfm:
        """
        df = self.dfm.create_msa_data_frame()
        make_msa = MakeMSA(df).get_msa_features()
        for loc_type in ['city', 'state']:
            make_msa['{}_stats'.format(loc_type)].to_csv('{0}location_characteristics_{1}.csv'.format(self.config['save_location'], loc_type),
                sep='\t',
                encoding='utf8')


    def run_ad_features(self, dfm):
        """
        Will run the ad features
        :param create_dataframe.DFManager dfm:
        """
        df = dfm.create_ad_dataframe()

        # Get the city and state features
        city_features = pd.read_table('{0}location_characteristics_city.csv'.format(self.config['save_location']))
        state_features = pd.read_table('{0}location_characteristics_state.csv'.format(self.config['save_location']))

        make_ad = MakeAd(city_features, state_features, df)
        ad_features = make_ad.get_ad_features()
        ad_features.to_csv('{0}ad_characteristics.csv'.format(self.config['save_location']),
                           sep='\t',
                           encoding='utf8',
                           index=False)


    def run_entity_features(self, dfm, entity):
        """

        :param create_dataframe.DFManager dfm:
        :param entity: The entity you'd like to do stats on
        """
        df = dfm.create_entity_dataframe(entity)
        make_entity = MakeEntity(df, entity).get_entity_features()
        make_entity.to_csv('{0}{1}_characteristics.csv'.format(self.config['save_location'], entity),
                           sep='\t',
                           encoding='utf8',
                           index=False)

    def main(self):

        # First create the directory that we will save everything to
        #mydir = os.path.join('home/ubuntu/memex_ad_features/data/', datetime.datetime.now().strftime('%Y%m%d'))
        #os.makedirs(mydir)

        """print('Getting location features...')
        self.run_location_features(self.dfm)
        print('Done!')"""

        print('Getting ad features...')
        self.run_ad_features(self.dfm)
        print('Done!')

        print('Getting (phone) entity features...')
        self.run_entity_features(self.dfm, 'phone')
        print('Done!')


if __name__ == "__main__":
    config = Parser().parse_config('config/config.conf')
    Run(config).main()

