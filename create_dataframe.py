import pandas
from make_msa import MakeMSA

config = {'age': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/age-combined.csv', ['ad_id', 'age']],
          'cbsa': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/cbsa-combined.csv', ['ad_id', 'msa_name', 'msa_type']],
          'ethnicities': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/ethnicities-dom.csv', ['ad_id', 'ethnicity']],
          'flags': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/flags.csv', ['ad_id', 'flag']],
          'rates': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/rates-text.csv', ['ad_id', 'rate']],
          'service': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/service-text.csv', ['ad_id', 'service']],
          'doc_id': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/doc_id_mapping.csv', ['ad_id', 'cdr_id']]}

class CreateDataFrame:
    def __init__(self, config):
        """

        :param config: A dictionary that contains the location of each file
        :return:
        """
        self.config = config

    def create_data_frame(self):
        """
        Will merge all the data by ad_id
        :return: Dataframe with all data merged
        """
        # First get all of the files we need to merge and make them into a dataframe
        age = self.get_data_frame_from_csv(self.config['age'][0], columns=self.config['age'][1])
        cbsa = self.get_data_frame_from_csv(self.config['cbsa'][0], columns=self.config['cbsa'][1])
        ethnicities = self.get_data_frame_from_csv(self.config['ethnicities'][0], columns=self.config['ethnicities'][1])
        flags = self.get_data_frame_from_csv(self.config['flags'][0], columns=self.config['flags'][1])
        rates = self.get_data_frame_from_csv(self.config['rates'][0], columns=self.config['rates'][1])
        service = self.get_data_frame_from_csv(self.config['service'][0], columns=self.config['service'][1])
        doc_id = self.get_data_frame_from_csv(self.config['doc_id'][0],  columns=self.config['doc_id'][1])

        # Next mege together by ad id
        return_dataframe = pandas.merge(doc_id, age, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, cbsa, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, ethnicities, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, flags, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, rates, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, service, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, doc_id, on='ad_id', how='left')

        return return_dataframe


    def get_data_frame_from_csv(self, filename, seperator='\t', columns=None):
        """
        Will a dataframe from a csv and set columns headers
        :param filename: Location of file
        :param seperator: Separator value
        :param columns: Column headers
        :return: Dataframe from csv
        """
        dataframe = pandas.read_csv(filename, sep=seperator)

        if columns:
            dataframe.columns = columns

        return dataframe

dataframe = CreateDataFrame(config).create_data_frame()
msa_features = MakeMSA(dataframe).get_msa_features()
msa_features.to_csv('test.csv')