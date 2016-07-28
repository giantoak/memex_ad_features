from create_dataframe import CreateDataFrame
from make_msa import MakeMSA
from make_ad import MakeAd
from make_entity import MakeEntity
import pandas
import json
import glob


#config = {'filenames': glob.glob('/home/ubuntu/2016_summer_camp/classifier/data/initial/lattice/*gz')}
config = {'filenames': ['subset.json.gz']}

class Run:
    def __init__(self):
        self.create_dataframe = CreateDataFrame(config)

    def run_location_features(self):
        """
        Will get features by city and state
        :return:
        """
        dataframe = self.create_dataframe.create_msa_data_frame()
        make_msa = MakeMSA(dataframe).get_msa_features()
        make_msa['city_stats'].to_csv('data/location_characteristics_city.csv', sep='\t', encoding='utf8')
        make_msa['state_stats'].to_csv('data/location_characteristics_state.csv', sep='\t', encoding='utf8')

    def run_ad_features(self):
        """
        Will run the ad features
        :return:
        """
        dataframe = self.create_dataframe.create_ad_dataframe()
        # Get the city and state features
        city_features = self.create_dataframe.get_data_frame_from_csv('data/location_characteristics_city.csv', seperator='\t')
        state_features = self.create_dataframe.get_data_frame_from_csv('data/location_characteristics_state.csv', seperator='\t')

        make_ad = MakeAd(city_features, state_features, dataframe)
        ad_features = make_ad.get_ad_features()
        ad_features.to_csv('data/ad_characteristics.csv', sep='\t')

    def run_entity_features(self, entity):
        """

        :param entity: The entity you'd like to do stats on
        :return:
        """
        dataframe = self.create_dataframe.create_entity_dataframe(entity)
        make_entity = MakeEntity(dataframe, entity).get_entity_features()
        make_entity.to_csv('data/phone_characteristics.csv', sep='\t')

    def run(self):
        """
        Will run all features
        :return:
        """
        self.run_location_features()
        print 'Saved location features'
        self.run_ad_features()
        print 'Saved ad features'
        self.run_entity_features('phone')
        print 'Saved entity features'

Run().run()

