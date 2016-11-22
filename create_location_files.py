import pandas
import glob
import cPickle
import os.path
import csv
import datetime
import gzip
from make_msa import MakeMSA
from make_ad import MakeAd
from make_entity import MakeEntity
from lattice_json_tools import gzipped_jsonline_file_to_df
from multiprocessing import Process, Queue, Lock, Pool
from config_parser import Parser
from random import randint


def create_location_files(file):
    """

    :return:
    """
    print 'Starting analyis for {0}'.format(file)
    start_time = datetime.datetime.now()
    lock.acquire()
    results = {'file': file,
               'start_time': start_time,
               'end_time': None,
               'total_time': None}

    if os.path.isfile(config['log_file']):
        with open(config['log_file'], 'a') as csv_file:
            field_names = ['file', 'start_time', 'end_time', 'total_time']
            writer = csv.DictWriter(csv_file, fieldnames=field_names)
            writer.writerow(results)
            csv_file.close()
    else:
        with open(config['log_file'], 'wb') as csv_file:
            field_names = ['file', 'start_time', 'end_time', 'total_time']
            writer = csv.DictWriter(csv_file, fieldnames=field_names)
            writer.writeheader()
            writer.writerow(results)
            csv_file.close()
    lock.release()

    # Get the dataframe from the provided file
    dataframe = gzipped_jsonline_file_to_df(file)

    # Drop duplicates
    dataframe.drop_duplicates()

    # Impute age and rate
    print 'Starting rate imputations for {0}'.format(file)
    X = cv_rate.transform(dataframe['content'])
    imputed_rate = rf_rate.predict(X)
    dataframe['imputed_rate'] = imputed_rate

    #print 'Starting age imputations for {0}'.format(file)
    X = cv_age.transform(dataframe['content'])
    imputed_age = rf_age.predict(X)
    dataframe['imputed_age'] = imputed_age


    print 'Imputations done'

    # Get data frames by city ids and then create a dictionary containing a city id as the key and a dataframe for that city as the value
    city_ids = dataframe.city_wikidata_id.unique()
    city_dataframe = {city_id : pandas.DataFrame() for city_id in city_ids}
    for key in city_dataframe.keys():
        city_dataframe[key] = dataframe[:][dataframe.city_wikidata_id == key]

    state_ids = dataframe.state_wikidata_id.unique()
    state_dataframe = {state_id: pandas.DataFrame() for state_id in state_ids}
    for key in state_dataframe.keys():
        state_dataframe[key] = dataframe[:][dataframe.state_wikidata_id == key]

    # Check if file already exists for each location, if so then append, if not then create a new file
    print 'Appending location data to existing files'

    # Lock all processes while work is being done to save files
    lock.acquire()
    print 'lock has been set for file {0}'.format(file)
    for key, value in city_dataframe.iteritems():
        if os.path.isfile('{0}city_id_{1}.csv'.format(config['location_data'], str(key))):
            value.to_csv('{0}city_id_{1}.csv'.format(config['location_data'], str(key)), mode='a', header=False, encoding='utf-8')
        else:
            value.to_csv('{0}city_id_{1}.csv'.format(config['location_data'], str(key)), header=True, encoding='utf-8')

    for key, value in state_dataframe.iteritems():
        if os.path.isfile('{0}state_id_{1}.csv'.format(config['location_data'], str(key))):
            value.to_csv('{0}state_id_{1}.csv'.format(config['location_data'], str(key)), mode='a', header=False, encoding='utf-8')
        else:
            value.to_csv('{0}state_id_{1}.csv'.format(config['location_data'], str(key)), header=True, encoding='utf-8')

    end_time = datetime.datetime.now()
    total_time = end_time - start_time
    results = {'file': file,
               'start_time': start_time,
               'end_time': end_time,
               'total_time': total_time}

    if os.path.isfile(config['log_file']):
        with open(config['log_file'], 'a') as csv_file:
            field_names = ['file', 'start_time', 'end_time', 'total_time']
            writer = csv.DictWriter(csv_file, fieldnames=field_names)
            writer.writerow(results)
            csv_file.close()
    else:
        with open(config['log_file'], 'wb') as csv_file:
            field_names = ['file', 'start_time', 'end_time', 'total_time']
            writer = csv.DictWriter(csv_file, fieldnames=field_names)
            writer.writeheader()
            writer.writerow(results)
            csv_file.close()

    print 'lock released for file {0}'.format(file)
    lock.release()

def initializeLock(l):
    """

    :param l: Lock
    :return:
    """

    # Lock needs to be globad for it to be passed to map
    global lock
    lock = l

def make_location_stas(file):

    if 'city' in file:
        file_type = 'city'
    else:
        file_type = 'state'

    print 'Starting analysis on {0}'.format(file)
    dataframe = pandas.read_csv(file)

    print '{0}'.format(len(dataframe))
    if (len(dataframe) > 100000):
        print 'Dataframe has size of {0}'.format(str(len(dataframe)))
        dataframe = dataframe.sample(n=100000)
        print 'Dataframe sample taken, new size is {0}'.format(str(len(dataframe)))

    make_msa = MakeMSA(dataframe)
    print 'Calculating stats for file {0}'.format(file)
    results = make_msa.get_msa_features(file_type)
    print 'finished file {0}'.format(file)
    #lock.acquire()
    if os.path.isfile('{0}location_characteristics_{1}.csv'.format(config['result_data'], file_type)):
        results.to_csv('{0}location_characteristics_{1}.csv'.format(config['result_data'], file_type), header=False, mode='a', encoding='utf-8')
    else:
        results.to_csv('{0}location_characteristics_{1}.csv'.format(config['result_data'], file_type), header=True, encoding='utf-8')
    #lock.release()

def make_ad_stats(file):

    # Get the dataframe from the provided file
    dataframe = pandas.read_csv(file)

    # Get the city and state location info
    city_dataframe = pandas.read_csv('{0}location_characteristics_city.csv'.format(config['result_data']))
    state_dataframe = pandas.read_csv('{0}location_characteristics_state.csv'.format(config['result_data']))

    make_ad = MakeAd(city_dataframe, state_dataframe, dataframe)
    results = make_ad.get_ad_features()
    lock.acquire()
    if os.path.isfile('{0}ad_characteristics.csv'.format(config['result_data'])):
        results.to_csv('{0}ad_characteristics.csv'.format(config['result_data']), header=False, mode='a', encoding='utf-8')
    else:
        results.to_csv('{0}ad_characteristics.csv'.format(config['result_data']), header=True, encoding='utf-8')
    lock.release()

def make_entity_stats(file):

    # Get the dataframe from the provided file
    entity = 'phone'
    dataframe = pandas.read_csv(file)

    make_entity = MakeEntity(dataframe, entity)
    results = make_entity.get_entity_features()

    if os.path.isfile('{0}{1}_characteristics.csv'.format(config['result_data'], entity)):
        results.to_csv('{0}{1}_characteristics.csv'.format(config['result_data'], entity), header=False, mode='a', encoding='utf8',index=False)
    else:
        results.to_csv('{0}{1}_characteristics.csv'.format(config['result_data'], entity), header=False, encoding='utf8', index=False)

def split_array(data, size=100):
    return [data[x:x+size] for x in xrange(0, len(data), size)]


def split_file(filename):
    count = 0
    outfile = gzip.open('{0}{1}_{2}.gz'.format(config['split_file_directory'], os.path.basename(filename), count), 'wb')
    for line in gzip.open(filename):
        if count % 100000 == 0:
            print '{0} lines read from {1}'.format(count, filename)
        if count % 500000 == 0:
            outfile.close()
            outfile = gzip.open('{0}{1}_{2}.gz'.format(config['split_file_directory'], os.path.basename(filename), count), 'wb')
        outfile.write(line)
        count += 1

    outfile.close()
    print '****** Finished {0}'.format(filename)

def count_line():
    count = 0
    for line in gzip.open('/home/gabriel/Documents/Memex/ad_features/flat_data/data_20160813-0000_1440_2016-07-20.json.gz'):
        print count
        count += 1


if __name__ == '__main__':
    # Load the configuration
    config = Parser().parse_config('config/config.conf', 'Test')

    # Before we begin processing we split the files to ensure no file has more than 500,000 lines.
    directory = '{0}'.format(config['flat_data'])
    file_names = glob.glob(directory)

    pool = Pool()
    pool.map(split_file, file_names)
    pool.close()
    pool.join()

    # Load the imputation models
    print 'Loading rate imputation models'
    cv_rate = cPickle.load(open(config['price_imputation_text_extractor_location'], 'rb'))
    rf_rate = cPickle.load(open(config['price_imputation_model_location'], 'rb'))
    print 'Loading age imputation modelsous'
    cv_age = cPickle.load(open(config['age_imputation_text_extractor_location'], 'rb'))
    rf_age = cPickle.load(open(config['age_imputation_model_location'], 'rb'))

    # Now we take all of the files that have been split and split them further by state wiki id and city wiki id
    directory = '{0}*.gz'.format(config['split_file_directory'])
    file_names = glob.glob(directory)

    lock = Lock()
    pool = Pool(initializer=initializeLock, initargs=(lock,), processes=3)
    pool.imap_unordered(create_location_files, file_names, 1)
    pool.close()
    pool.join()

    # Calculate stats for each location
    directory = '{0}*.csv'.format(config['location_data'])
    file_names = glob.glob(directory)

    lock = Lock()
    pool = Pool(initializer=initializeLock, initargs=(lock,), processes=3)
    pool.imap_unordered(make_location_stas, file_names)
    pool.close()
    pool.join()

    # Now that we have the location data. Let's calculate stats for each ad.
    # Since the same ads are in the city and state file, let's only pull from the city files
    directory = '{0}city*.csv'.format(config['location_data'])
    file_names = glob.glob(directory)
    lock = Lock()
    pool = Pool(initializer=initializeLock, initargs=(lock,), processes=3)
    pool.imap_unordered(make_ad_stats, file_names)
    pool.close()
    pool.join()

    # Lastly, calculate phone stats
    directory = '{0}city*.csv'.format(config['location_data'])
    file_names = glob.glob(directory)
    lock = Lock()
    pool = Pool(initializer=initializeLock, initargs=(lock,), processes=3)
    pool.imap_unordered(make_entity_stats, file_names)
    pool.close()
    pool.join()

