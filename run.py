from create_dataframe import CreateDataFrame
from make_msa import MakeMSA
from make_ad import MakeAd
from make_entity import MakeEntity
import pandas

config = {'age': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/age-combined.csv', ['ad_id', 'age']],
          'cbsa': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/cbsa-combined.csv', ['ad_id', 'msa_name', 'msa_type']],
          'ethnicities': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/ethnicities-dom.csv', ['ad_id', 'ethnicity']],
          'flags': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/flags.csv', ['ad_id', 'flag']],
          'rates': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/rates-text.csv', ['ad_id', 'rate']],
          'service': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/service-text.csv', ['ad_id', 'service']],
          'doc_id': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/doc_id_mapping.csv', ['ad_id', 'cdr_id']],
          'post_date': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/post_date-dom.csv', ['ad_id', 'post_date']],
          'phones': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/phones-combined.csv', ['ad_id', 'phone']]}

class Run:
    def __init__(self):
        self.create_dataframe = CreateDataFrame(config)

    def run_msa_features(self):
        dataframe = self.create_dataframe.create_msa_data_frame()
        make_msa = MakeMSA(dataframe).get_msa_features()
        make_msa.to_csv('data/msa_characteristics.csv', sep='\t')

    def run_ad_features(self):
        dataframe = self.create_dataframe.create_ad_dataframe()
        msa_features = self.create_dataframe.get_data_frame_from_csv('data/msa_characteristics.csv', seperator='\t')
        make_ad = MakeAd(msa_features, dataframe)
        ad_features = make_ad.get_ad_features()
        ad_features.to_csv('data/ad_characteristics.csv', sep='\t')

    def run_entity_features(self, entity):
        dataframe = self.create_dataframe.create_entity_dataframe(entity)
        make_entity = MakeEntity(dataframe, 'phone').get_entity_features()



Run().run_entity_features('phones')

