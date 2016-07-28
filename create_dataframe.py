from distutils.command.config import config

import datetime
import pandas as pd
import ujson as json
import gzip


class CreateDataFrame:
    def __init__(self, config):
        """

        :param config: A dictionary that contains the location of each file
        :return:
        """
        self.config = config
        self.dataframe = pd.DataFrame()
        self.city_append = pd.DataFrame()
        self.state_append = pd.DataFrame()
        self.age_append = pd.DataFrame()
        self.entity_append = pd.DataFrame()

        jsns = []
        for filename in config['filenames'][0:2]:
            print(filename)
            print(datetime.datetime.now())
            data = [json.loads(line) for line in gzip.open(filename)]
            dataframe = pd.DataFrame(data[0])
            #del dataframe['title']
            #for i in dataframe.columns:
                #if '_lat_' in i:
                    #del dataframe[i]
            self.dataframe = self.dataframe.append(dataframe, ignore_index=True)
            ipdb.set_trace()

    def create_msa_data_frame(self):
        """
        Will merge all the data by ad_id
        :return: Dataframe with all data merged
        """
        # Reset the dataframes we use for apending in case this class has already been used
        self.reset_append_dataframes()

        dataframe = pd.DataFrame()
        dataframe['rate'] = self.dataframe['rate']
        dataframe['age'] = self.dataframe['age']
        dataframe['city'] = self.dataframe['location_city_name']
        dataframe['state'] = self.dataframe['location_state_name']

        # Since the dataframe contains some values with lists, we have to break those out. To do this, we will take all lists with length of 1 and return the only member of the list
        # For lists with length > 1 we will append their value to self.city_append and then append self.city_append to self.msa_rate
        dataframe['city'] = dataframe.apply(lambda x: self.convert_from_list_city_rate(x['city'], x['state'], x['rate'], x['age']), axis=1)
        dataframe = dataframe.append(self.city_append, ignore_index=True)

        # Do the same for the states
        dataframe['state'] = dataframe.apply(lambda x: self.convert_from_list_state_rate(x['city'], x['state'], x['rate'], x['age']), axis=1)
        dataframe = dataframe.append(self.state_append, ignore_index=True)

        # Do the same with ages
        dataframe['age'] = dataframe.apply(lambda x: self.convert_from_list_age(x['age'], x['rate'], x['city'], x['state']), axis=1)
        dataframe = dataframe.append(self.age_append, ignore_index=True)

        return dataframe

    def create_ad_dataframe(self):
        # Reset the dataframes we use for apending in case this class has already been used
        self.reset_append_dataframes()

        dataframe = pd.DataFrame()
        dataframe['rate'] = self.dataframe['rate']
        dataframe['cdr_id'] = self.dataframe['_id']
        dataframe['city'] = self.dataframe['location_city_name']
        dataframe['state'] = self.dataframe['location_state_name']

        # Since the dataframe contains some values with lists, we have to break those out. To do this, we will take all lists with length of 1 and return the only member of the list
        # For lists with length > 1 we will append their value to self.city_append and then append self.city_append to self.msa_rate
        dataframe['city'] = dataframe.apply(lambda x: self.convert_from_list_city_ad(x['city'], x['state'], x['rate'], x['cdr_id']), axis=1)
        dataframe = dataframe.append(self.city_append, ignore_index=True)

        # Do the same for the states
        dataframe['state'] = dataframe.apply(lambda x: self.convert_from_list_state_ad(x['city'], x['state'], x['rate'], x['cdr_id']), axis=1)
        dataframe = dataframe.append(self.state_append, ignore_index=True)

        return dataframe

    def create_entity_dataframe(self, entity):
        # Reset the dataframes we use for apending in case this class has already been used
        self.reset_append_dataframes()

        dataframe = pd.DataFrame()
        dataframe['rate'] = self.dataframe['rate']
        dataframe['cdr_id'] = self.dataframe['_id']
        dataframe['city'] = self.dataframe['location_city_name']
        dataframe['state'] = self.dataframe['location_state_name']
        dataframe[entity] = self.dataframe[entity]

        # Since the dataframe contains some values with lists, we have to break those out. To do this, we will take all lists with length of 1 and return the only member of the list
        # For lists with length > 1 we will append their value to self.city_append and then append self.city_append to self.msa_rate
        dataframe['city'] = dataframe.apply(lambda x: self.convert_from_list_city_entity(x['city'], x['state'], x['rate'], x['cdr_id'], x[entity], entity), axis=1)
        dataframe = dataframe.append(self.city_append, ignore_index=True)

        # Do the same for the states
        dataframe['state'] = dataframe.apply(lambda x: self.convert_from_list_state_entity(x['city'], x['state'], x['rate'], x['cdr_id'], x[entity], entity), axis=1)
        dataframe = dataframe.append(self.state_append, ignore_index=True)

        dataframe[entity] = dataframe.apply(lambda x: self.convert_from_list_entity(x['city'], x['state'], x['rate'], x['cdr_id'], x[entity], entity), axis=1)
        dataframe = dataframe.append(self.entity_append, ignore_index=True)


        return dataframe

    def get_data_frame_from_csv(self, filename, seperator='\t', columns=None):
        """
        Will a dataframe from a csv and set columns headers
        :param filename: Location of file
        :param seperator: Separator value
        :param columns: Column headers
        :return: Dataframe from csv
        """
        dataframe = pd.read_csv(filename, sep=seperator)

        if columns:
            dataframe.columns = columns

        return dataframe

    def convert_from_list_city_rate(self, city, state, rate, age):
        if type(city) is list:
            if len(city) == 1:
                return city[0]
            else:
                for i in xrange(1, len(city), 1):
                    self.city_append = self.city_append.append(pd.DataFrame([[city[i], state, rate, age]], columns=['city', 'state', 'rate', 'age']), ignore_index=True)
                return city[0]
        else:
            return city

    def convert_from_list_state_rate(self, city, state, rate, age):
        if type(state) is list:
            if len(state) == 1:
                return state[0]
            else:
                for i in xrange(1, len(state), 1):
                    self.state_append = self.state_append.append(pd.DataFrame([[city, state[i], rate, age]], columns=['city', 'state', 'rate', 'age']), ignore_index=True)
                return state[0]
        else:
            return state

    def convert_from_list_age(self, age, rate, city, state):
        if type(age) is list:
            if len(age) == 1:
                return float(age[0])
            else:
                for i in xrange(1, len(age), 1):
                    self.age_append = self.age_append.append(pd.DataFrame([[float(age[i]), rate, city, state]], columns=['age', 'rate', 'city', 'state']), ignore_index=True)
        else:
            return age


    def convert_from_list_city_ad(self, city, state, rate, cdr_id):
        if type(city) is list:
            if len(city) == 1:
                return city[0]
            else:
                for i in xrange(1, len(city), 1):
                    self.city_append = self.city_append.append(pd.DataFrame([[city[i], state, rate, cdr_id]], columns=['city', 'state', 'rate', 'cdr_id']), ignore_index=True)
                return city[0]
        else:
            return city

    def convert_from_list_state_ad(self, city, state, rate, cdr_id):
        if type(state) is list:
            if len(state) == 1:
                return state[0]
            else:
                for i in xrange(1, len(state), 1):
                    self.state_append = self.state_append.append(pd.DataFrame([[city, state[i], rate, cdr_id]], columns=['city', 'state', 'rate', 'cdr_id']), ignore_index=True)
                return state[0]
        else:
            return state

    def convert_from_list_city_entity(self, city, state, rate, cdr_id, entity_value, entity_name):
        if type(city) is list:
            if len(city) == 1:
                return city[0]
            else:
                for i in xrange(1, len(city), 1):
                    self.city_append = self.city_append.append(pd.DataFrame([[city[i], state, rate, cdr_id, entity_value]], columns=['city', 'state', 'rate', 'cdr_id', entity_name]), ignore_index=True)
                return city[0]
        else:
            return city

    def convert_from_list_state_entity(self, city, state, rate, cdr_id, entity_value, entity_name):
        if type(state) is list:
            if len(state) == 1:
                return state[0]
            else:
                for i in xrange(1, len(state), 1):
                    self.state_append = self.state_append.append(pd.DataFrame([[city, state[i], rate, cdr_id, entity_value]], columns=['city', 'state', 'rate', 'cdr_id', entity_name]), ignore_index=True)
                return state[0]
        else:
            return state

    def convert_from_list_entity(self, city, state, rate, cdr_id, entity_value, entity_name):
        if type(entity_value) is list:
            if len(entity_value) == 1:
                return entity_value[0]
            else:
                for i in xrange(1, len(entity_value), 1):
                    self.entity_append = self.entity_append.append(pd.DataFrame([[city, state, rate, cdr_id, entity_value[i]]], columns=['city', 'state', 'rate', 'cdr_id', entity_name]), ignore_index=True)
                return entity_value[0]
        else:
            return entity_value

    def reset_append_dataframes(self):
        self.city_append = pd.DataFrame()
        self.state_append = pd.DataFrame()
        self.age_append = pd.DataFrame()
        self.entity_append = pd.DataFrame()

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
