from distutils.command.config import config

import pandas
import csv
from make_msa import MakeMSA

"""config = {'age': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/age-combined.tsv', ['ad_id', 'age']],
          'cbsa': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/cbsa-combined.tsv', ['ad_id', 'msa_name', 'msa_type']],
          'ethnicities': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/ethnicities-dom.tsv', ['ad_id', 'ethnicity']],
          'flags': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/flags.tsv', ['ad_id', 'flag']],
          'rates': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/rates-text.tsv', ['ad_id', 'rate']],
          'service': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/service-text.tsv', ['ad_id', 'service']],
          'doc_id': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/doc_id_mapping.tsv', ['ad_id', 'cdr_id']],
          'post_date': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/post_date-dom.tsv', ['ad_id', 'post_date']]}"""

"""config = {'age': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/age-combined.csv', ['ad_id', 'age']],
          'cbsa': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/cbsa-combined.csv', ['ad_id', 'msa_name', 'msa_type']],
          'ethnicities': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/ethnicities-dom.csv', ['ad_id', 'ethnicity']],
          'flags': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/flags.csv', ['ad_id', 'flag']],
          'rates': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/rates-text.csv', ['ad_id', 'rate']],
          'service': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/service-text.csv', ['ad_id', 'service']],
          'doc_id': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/doc_id_mapping.csv', ['ad_id', 'cdr_id']],
          'post_date': ['/media/gabriel/My Passport/memex/escort_cdr_2/cdr/subset/post_date-dom.csv', ['ad_id', 'post_date']]}"""

class CreateDataFrame:
    def __init__(self, config):
        """

        :param config: A dictionary that contains the location of each file
        :return:
        """
        self.config = config
        # First get all of the files we need to merge and make them into a dataframe
        self.age = self.get_data_frame_from_csv(self.config['age'][0], columns=self.config['age'][1])
        self.cbsa = self.get_data_frame_from_csv(self.config['cbsa'][0], columns=self.config['cbsa'][1])
        self.ethnicities = self.get_data_frame_from_csv(self.config['ethnicities'][0], columns=self.config['ethnicities'][1])
        self.flags = self.get_data_frame_from_csv(self.config['flags'][0], columns=self.config['flags'][1])
        self.rates = self.get_data_frame_from_csv(self.config['rates'][0], columns=self.config['rates'][1])
        self.service = self.get_data_frame_from_csv(self.config['service'][0], columns=self.config['service'][1])
        self.doc_id = self.get_data_frame_from_csv(self.config['doc_id'][0], columns=self.config['doc_id'][1])
        self.post_date = self.get_data_frame_from_csv(self.config['post_date'][0], columns=self.config['post_date'][1])
        #self.phone = self.get_data_frame_from_csv(self.config['phone'][0], columns=self.config['phone'][1])

    def create_msa_data_frame(self):
        """
        Will merge all the data by ad_id
        :return: Dataframe with all data merged
        """

        # Merge together by ad id
        return_dataframe = pandas.merge(self.doc_id, self.rates, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, self.cbsa, on='ad_id', how='left')
        #return_dataframe = pandas.merge(return_dataframe, ethnicities, on='ad_id', how='left')
        #return_dataframe = pandas.merge(return_dataframe, flags, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, self.age, on='ad_id', how='left')
        #return_dataframe = pandas.merge(return_dataframe, service, on='ad_id', how='left')
        #return_dataframe = pandas.merge(return_dataframe, doc_id, on='ad_id', how='left')
        #return_dataframe = pandas.merge(return_dataframe, post_date, on='ad_id', how='left')

        return return_dataframe

    def create_ad_dataframe(self):
        # Merge together by ad id
        return_dataframe = pandas.merge(self.doc_id, self.rates, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, self.cbsa, on='ad_id', how='left')

        return return_dataframe

    def create_entity_dataframe(self, entity):
        entity_dataframe = self.get_data_frame_from_csv(self.config[entity][0], columns=self.config[entity][1])
        return_dataframe = pandas.merge(self.doc_id, entity_dataframe, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, self.service, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, self.cbsa, on='ad_id', how='left')
        return_dataframe = pandas.merge(return_dataframe, self.rates, on='ad_id', how='left')

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

    """def strip_phone(self):
        return_data = []
        with open('/home/gabriel/phone.csv') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=',')
            for result in reader:
                if '|' in result['phone']:
                    phone_numbers = result['phone'].split('|')
                    for number in phone_numbers:
                        data = {}
                        number = number.strip()
                        data['ad_id'] = result['ad_id']
                        data['phone'] = number
                        return_data.append(data)
                else:
                    data = {}
                    data['ad_id'] = result['ad_id']
                    data['phone'] = result['phone']
                    return_data.append(data)

        with open ('/home/gabriel/phone_fixed.csv', 'wb') as f:
            w = csv.DictWriter(f, fieldnames=['ad_id', 'phone'], extrasaction='ignore')
            w.writeheader()
            w.writerows(return_data)
            f.close()"""


#dataframe = CreateDataFrame(config).create_data_frame()
#msa_features = MakeMSA(dataframe).get_msa_features()