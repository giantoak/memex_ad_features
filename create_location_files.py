import pandas
import glob
import cPickle
import os.path
import csv
from lattice_json_tools import gzipped_jsonline_file_to_df
from config_parser import Parser

class CreateLocationFiles:
    def __init__(self, config):
        self.config = config

    def create_location_files(self, directory):
        """

        :return:
        """

        # Load the imputation models
        print 'Loading rate imputation models'
        cv_rate = cPickle.load(open(self.config['price_imputation_text_extractor_location'], 'rb'))
        rf_rate = cPickle.load(open(self.config['price_imputation_model_location'], 'rb'))
        print 'Loading age imputation models'
        cv_age = cPickle.load(open(self.config['age_imputation_text_extractor_location'], 'rb'))
        rf_age = cPickle.load(open(self.config['age_imputation_model_location'], 'rb'))

        cols_to_use = ['_id',
                       'age',
                       'city',
                       'state',
                       'city_wikidata_id',
                       'state_wikidata_id',
                       'phone',
                       'content',
                       'rate',
                       'imputed_rate',
                       'imputed_age']

        # First go through each file and save the location info in a separate file by location id
        print 'Getting all files from given directory'
        file_names = glob.glob(directory)

        # Get the city and state counts
        city_counts = self.get_city_counts()
        state_counts = self.get_state_counts()

        print 'Starting file analysis'
        for file in file_names:
            print 'Starting analyis for {0}'.format(file)
            # Save the contents into a dictionary by state and city wiki id
            city_dataframe = {}
            state_dataframe = {}

            city_contents = {}
            state_contents = {}

            dataframe = gzipped_jsonline_file_to_df(file)

            # Drop duplicates
            print 'Dropping duplicates'
            dataframe.drop_duplicates()

            # Impute age and rate
            print 'Starting rate imputations for {0}'.format(file)
            X = cv_rate.transform(dataframe['content'])
            imputed_rate = rf_rate.predict(X)
            dataframe['imputed_rate'] = imputed_rate

            print 'Starting age imputations for {0}'.format(file)
            X = cv_age.transform(dataframe['content'])
            imputed_age = rf_age.predict(X)
            dataframe['imputed_age'] = imputed_age

            print 'Imputations done'

            city_ids = dataframe.city_wikidata_id.unique()
            city_dataframe = {city_id : pandas.DataFrame() for city_id in city_ids}

            for key in city_dataframe.keys():
                city_dataframe[key] = dataframe[:][dataframe.city_wikidata_id == key]

            state_ids = dataframe.state_wikidata_id.unique()
            state_dataframe = {state_id: pandas.DataFrame() for state_id in state_ids}

            for key in state_dataframe.keys():
                state_dataframe[key] = dataframe[:][dataframe.state_wikidata_id == key]

            # Now go through and ad to dictionary by wiki id
            """print 'Splitting file {0} with {1} rows by city and state wiki ids'.format(file, str(len(dataframe)))
            for index, row in dataframe.iterrows():
                city_id = row['city_wikidata_id']
                state_id = row['state_wikidata_id']

                # We want all of our indexes to be unique, so let's add the total amount of records to the index each time
                index_city_add = city_counts.get(city_id, 0)
                index_state_add = state_counts.get(state_id, 0)

                if city_contents.get(city_id, None):
                    city_dataframe[city_id].loc[len(city_dataframe[city_id]) + index_city_add] = row
                else:
                    temp_dataframe = pandas.DataFrame(columns = cols_to_use)
                    temp_dataframe.loc[index_city_add] = row
                    city_dataframe[city_id] = temp_dataframe
                    city_contents[city_id] = True

                if state_contents.get(state_id, None):
                    state_dataframe[state_id].loc[len(state_dataframe[state_id]) + index_state_add] = row
                else:
                    temp_dataframe = pandas.DataFrame(columns=cols_to_use)
                    temp_dataframe.loc[index_state_add] = row
                    state_dataframe[state_id] = temp_dataframe
                    state_contents[state_id] = True"""

            # Check if file already exists for each location, if so then append, if not then create a new file
            print 'Appending location data to existing files'
            for key, value in city_dataframe.iteritems():
                index_city_add = city_counts.get(key, 0)
                index_city_add += len(city_dataframe[key])
                city_counts[key] = index_city_add
                if os.path.isfile('{0}city_id_{1}.csv'.format(self.config['location_data'], str(key))):
                    value.to_csv('{0}city_id_{1}.csv'.format(self.config['location_data'], str(key)), mode='a', header=False, encoding='utf-8')
                else:
                    value.to_csv('{0}city_id_{1}.csv'.format(self.config['location_data'], str(key)), header=True, encoding='utf-8')

            for key, value in state_dataframe.iteritems():
                index_state_add = state_counts.get(key, 0)
                index_state_add += len(state_dataframe[key])
                state_counts[key] = index_state_add
                if os.path.isfile('{0}state_id_{1}.csv'.format(self.config['location_data'], str(key))):
                    value.to_csv('{0}state_id_{1}.csv'.format(self.config['location_data'], str(key)), mode='a', header=False, encoding='utf-8')
                else:
                    value.to_csv('{0}state_id_{1}.csv'.format(self.config['location_data'], str(key)), header=True, encoding='utf-8')

        self.set_city_counts(city_counts)
        self.set_state_counts(state_counts)


    def get_city_counts(self):
        city_counts = {}
        # First get the file that contains the counts for each location
        if os.path.isfile(self.config['city_counts']):
            with open(self.config['city_counts']) as csv_file:
                reader = csv.reader(csv_file)
                city_ids = next(reader)
                counts = next(reader)
                for i in xrange(0, len(city_ids)):
                    city_counts[float(city_ids[i])] = float(counts[i])

        return city_counts

    def get_state_counts(self):
        state_counts = {}
        # First get the file that contains the counts for each location
        if os.path.isfile(self.config['state_counts']):
            with open(self.config['state_counts']) as csv_file:
                reader = csv.reader(csv_file)
                state_ids = next(reader)
                counts = next(reader)
                for i in xrange(0, len(state_ids)):
                    state_counts[float(state_ids[i])] = float(counts[i])

        return state_counts

    def set_city_counts(self, city_counts):
        contents = []
        contents.append(city_counts)
        if os.path.isfile(self.config['city_counts']):
            os.remove(self.config['city_counts'])
        with open(self.config['city_counts'], 'wb') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=city_counts.keys())
            writer.writeheader()
            writer.writerows(contents)
            csv_file.close()

    def set_state_counts(self, state_counts):
        contents = []
        contents.append(state_counts)
        if os.path.isfile(self.config['state_counts']):
            os.remove(self.config['state_counts'])
        with open(self.config['state_counts'], 'wb') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=state_counts.keys())
            writer.writeheader()
            writer.writerows(contents)
            csv_file.close()


config = Parser().parse_config('config/config.conf', 'Test')
CreateLocationFiles(config).create_location_files('/home/gabriel/Documents/Memex/ad_features/ad_data/*.gz')