import pandas
import glob
import cPickle
import os.path
import csv
import time
from make_msa import MakeMSA
from make_ad import MakeAd
from lattice_json_tools import gzipped_jsonline_file_to_df
from multiprocessing import Process, Queue, Lock, Pool
from config_parser import Parser
from random import randint


def create_location_files(file):
    """

    :return:
    """

    #time.sleep(randint(0,10))

    print 'Starting analyis for {0}'.format(file)

    # Get the dataframe from the provided file
    dataframe = gzipped_jsonline_file_to_df(file)

    # Drop duplicates
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

    dataframe = pandas.read_csv(file)

    if (len(dataframe) > 100000):
        dataframe = dataframe.sample(n=100000)

    make_msa = MakeMSA(dataframe)
    results = make_msa.get_msa_features(file_type)
    print 'finished file {0}'.format(file)
    lock.acquire()
    if os.path.isfile('{0}location_characteristics_{1}.csv'.format(config['result_data'], file_type)):
        results.to_csv('{0}location_characteristics_{1}.csv'.format(config['result_data'], file_type), header=False, mode='a', encoding='utf-8')
    else:
        results.to_csv('{0}location_characteristics_{1}.csv'.format(config['result_data'], file_type), header=True, encoding='utf-8')
    lock.release()

def make_ad_stats(file):
    # Get the dataframe from the provided file
    if 'city' not in file:
        return

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

def split_array(data, size=100):
    return [data[x:x+size] for x in xrange(0, len(data), size)]

if __name__ == '__main__':
    config = Parser().parse_config('config/config.conf', 'AWS')
    # Load the imputation models
    print 'Loading rate imputation models'
    cv_rate = cPickle.load(open(config['price_imputation_text_extractor_location'], 'rb'))
    rf_rate = cPickle.load(open(config['price_imputation_model_location'], 'rb'))
    print 'Loading age imputation modelsous'
    cv_age = cPickle.load(open(config['age_imputation_text_extractor_location'], 'rb'))
    rf_age = cPickle.load(open(config['age_imputation_model_location'], 'rb'))

    directory = '/home/ubuntu/split_files/*.gz'
    file_names = glob.glob(directory)

    lock = Lock()
    pool = Pool(initializer=initializeLock, initargs=(lock,))
    pool.map(create_location_files, file_names)
    pool.close()
    pool.join()

    # Calculate stats for each location
    # directory = '/home/gabriel/Documents/Memex/ad_features/location_data/*'
    # file_names = glob.glob(directory)
    #
    # lock = Lock()
    # pool = Pool(initializer=initializeLock, initargs=(lock,))
    # pool.map(make_location_stas, file_names)
    # pool.close()
    # pool.join()
    #
    #
    # directory = '/home/gabriel/Documents/Memex/ad_features/location_data/*'
    # file_names = glob.glob(directory)
    # lock = Lock()
    # pool = Pool(initializer=initializeLock, initargs=(lock,))
    # pool.map(make_ad_stats, file_names)
    # pool.close()
    # pool.join()

    # directory = '/home/ubuntu/flat_data/data*.gz'
    # file_names = glob.glob(directory)
    #
    # lock = Lock()
    # count = 0
    # processes = []
    # for file in file_names:
    #     count += 1
    #     p = Process(target=create_location_files, args=(file,))
    #     p.start()
    #     processes.append(p)
    #
    #     if count % 100 == 100:
    #         for process in processes:
    #             p.join()
    #         processes = []
    #
    # for process in processes:
    #     p.join()
