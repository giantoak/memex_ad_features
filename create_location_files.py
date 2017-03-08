import pandas
import glob
import cPickle
import os.path
import gzip
import multiprocessing
import re
from make_msa import MakeMSA
from make_ad import MakeAd
from make_entity import MakeEntity
from lattice_json_tools import gzipped_jsonline_file_to_df
from multiprocessing import Lock, Pool, Queue, Process
from config_parser import Parser
import ipdb
from model_munge import ItemSelector
from model_munge import Summarizer

def split_file(filename):
    """

    :param filename:
    :return:
    """
    count = 0
    outfile = gzip.open('{0}{1}_{2}.gz'.format(config['split_file_directory'], os.path.basename(filename), count), 'wb')
    for line in gzip.open(filename):
        if count % 500000 == 0:
            outfile.close()
            outfile = gzip.open('{0}{1}_{2}.gz'.format(config['split_file_directory'], os.path.basename(filename), count), 'wb')
        outfile.write(line)
        count += 1

    outfile.close()
    print '****** Finished {0}'.format(filename)


def create_location_files(file):
    """

    :param file:
    :return:
    """
    print 'Starting analyis for {0}'.format(file)
    process_id = multiprocessing.current_process().pid

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


    print 'Imputations done for {0}'.format(file)

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
    for key, value in city_dataframe.iteritems():
        if os.path.isfile('{0}city_id_{1}_{2}.csv'.format(config['location_data'], str(key), str(process_id))):
            value.to_csv('{0}city_id_{1}_{2}.csv'.format(config['location_data'], str(key), str(process_id)), mode='a', header=False, encoding='utf-8')
        else:
            value.to_csv('{0}city_id_{1}_{2}.csv'.format(config['location_data'], str(key), str(process_id)), header=True, encoding='utf-8')

    for key, value in state_dataframe.iteritems():
        if os.path.isfile('{0}state_id_{1}_{2}.csv'.format(config['location_data'], str(key), str(process_id))):
            value.to_csv('{0}state_id_{1}_{2}.csv'.format(config['location_data'], str(key), str(process_id)), mode='a', header=False, encoding='utf-8')
        else:
            value.to_csv('{0}state_id_{1}_{2}.csv'.format(config['location_data'], str(key), str(process_id)), header=True, encoding='utf-8')

    print '*******************Finished file {0}********************************'.format(file)


def make_location_stats(file):
    """

    :param file:
    :return:
    """
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

    if os.path.isfile('{0}location_characteristics_{1}.csv'.format(config['result_data'], file_type)):
        lock.acquire()
        results.to_csv('{0}location_characteristics_{1}.csv'.format(config['result_data'], file_type), header=False, mode='a', encoding='utf-8')
        lock.release()
    else:
        results.to_csv('{0}location_characteristics_{1}.csv'.format(config['result_data'], file_type), header=True, encoding='utf-8')


def make_ad_stats(file):
    """

    :param file:
    :return:
    """
    # Get the dataframe from the provided file
    dataframe = pandas.read_csv(file, index_col=0)

    # Get the city and state location info
    city_dataframe = pandas.read_csv('{0}location_characteristics_city.csv'.format(config['result_data']))
    state_dataframe = pandas.read_csv('{0}location_characteristics_state.csv'.format(config['result_data']))

    make_ad = MakeAd(city_dataframe, state_dataframe, dataframe)
    results = make_ad.get_ad_features()

    if os.path.isfile('{0}ad_characteristics.csv'.format(config['result_data'])):
        lock.acquire()
        results.to_csv('{0}ad_characteristics.csv'.format(config['result_data']), header=False, mode='a', encoding='utf-8')
        lock.release()
    else:
        results.to_csv('{0}ad_characteristics.csv'.format(config['result_data']), header=True, encoding='utf-8')


def make_entity_stats(file):
    """

    :param file:
    :return:
    """
    # Get the dataframe from the provided file
    entity = 'phone'
    dataframe = pandas.read_csv(file)

    make_entity = MakeEntity(dataframe, entity)
    results = make_entity.get_entity_features()

    if os.path.isfile('{0}{1}_characteristics.csv'.format(config['result_data'], entity)):
        lock.acquire()
        results.to_csv('{0}{1}_characteristics.csv'.format(config['result_data'], entity), header=False, mode='a', encoding='utf8',index=False)
        lock.release()
    else:
        results.to_csv('{0}{1}_characteristics.csv'.format(config['result_data'], entity), header=True, encoding='utf8', index=False)

def initializeLock(l):
    """

    :param l: Lock
    :return:
    """
    # Lock needs to be global for it to be passed to map
    global lock
    lock = l


def merge_files(base_file_name):
    """

    :param base_file_name:
    :return:
    """
    write_directory = config['location_data_merged']
    all_files = glob.glob('{0}*'.format(base_file_name))

    is_file_created = False

    for file in all_files:
        if is_file_created:
            print 'File already exists, appending'
            dataframe = pandas.read_csv(file, index_col=0)
            dataframe.to_csv('{0}{1}.csv'.format(write_directory, os.path.basename(base_file_name)), mode='a', header=False, encoding='utf-8')
        else:
            print 'File does not exist, creating.'
            dataframe = pandas.read_csv(file, index_col=0)
            dataframe.to_csv('{0}{1}.csv'.format(write_directory, os.path.basename(base_file_name)), header=True, encoding='utf-8')
            is_file_created = True

    print 'Finished merging {0}'.format(base_file_name)


def get_unique_base_file_names(directory):
    all_files_list = glob.glob(directory)
    all_files_dict = {}
    return_files = []

    for file in all_files_list:
        base_file = get_base_file_name(file)
        if all_files_dict.get(base_file, None):
            pass
        else:
            all_files_dict[base_file] = True

    for key, value in all_files_dict.iteritems():
        return_files.append(key)

    return return_files


def get_base_file_name(file_name):
    index = file_name.rfind('_')
    return file_name[:index]

def get_ht_score(file):
    dataframe = pandas.read_csv(file)
    dataframe.rename(columns={'phone': 'phone_1', 'imputed_rate': 'price_imputed', 'imputed_age': 'age_imputed'}, inplace=True)

    ht_probs = pipeline.predict_proba(dataframe)[:, 1]
    dict_result = {'ht_score': ht_probs[0]}
    result = pandas.DataFrame(index=[dataframe.get_value(0, 1, True)], data=dict_result)

    print 'Adding data to queue'
    worker_queue.put(result)


def create_phone_files(dataframe):
    # Drop all rows without a phone number
    dataframe = dataframe.dropna(subset=['phone'])
    dataframe['phone'] = dataframe['phone'].map(lambda x: re.sub('[^0-9]', '', str(x)))

    # Break file up by phone
    phone_numbers = dataframe.phone.unique()
    phone_dataframe = {phone_number: pandas.DataFrame() for phone_number in phone_numbers}
    for key in phone_dataframe.keys():
        phone_dataframe[key] = dataframe[:][dataframe.phone == key]

    # Check if file already exists for each location, if so then append, if not then create a new file
    print 'Appending location data to existing files'

    # Lock all processes while work is being done to save files
    for key, value in phone_dataframe.iteritems():
        if os.path.isfile('{0}phone_{1}.csv'.format(config['phone_data'], str(key))):
            lock.acquire()
            print 'lock has been set for file {0}'.format(file)
            value.to_csv('{0}phone_{1}.csv'.format(config['phone_data'], str(key)), mode='a', header=False, encoding='utf-8')
            lock.release()
        else:
            value.to_csv('{0}phone_{1}.csv'.format(config['phone_data'], str(key)), header=True, encoding='utf-8')
    print 'finished file {0}'.format(file)


def apply_ht_scores(dataframe):
    # Load the ht score dataframe
    ipdb.set_trace()
    ht_scores = pandas.read_csv('{0}ht_scores.csv'.format(config['result_data']), index_col=0)
    dataframe['phone'] = dataframe['phone'].map(lambda x: re.sub('[^0-9]', '', str(x)))
    # Make the column a numeric column for merging
    dataframe['phone'] = pandas.to_numeric(dataframe['phone'])
    final = dataframe.merge(ht_scores, how='left', left_on='phone', right_index=True)

    # Drop the content column and drop the index column
    final.drop('content', axis=1, inplace=True)

    if os.path.isfile('{0}ad_chars_final.csv'.format(config['result_data'])):
        lock.acquire()
        print 'lock has been set for file {0}'.format(file)
        final.to_csv('{0}ad_chars_final.csv'.format(config['result_data']), mode='a', header=False, encoding='utf-8', index=False)
        lock.release()
    else:
        final.to_csv('{0}ad_chars_final.csv'.format(config['result_data']), header=True, encoding='utf-8', index=False)

def append_ht_scores(worker_queue, ender_queue):
    append_count = 0
    while True:
        if worker_queue.empty() and not ender_queue.empty():
            if append_count > 0:
                if os.path.isfile('{0}ht_scores.csv'.format(config['result_data'])):
                    dataframe.to_csv('{0}ht_scores.csv'.format(config['result_data']), mode='a', header=False, encoding='utf-8')
                else:
                    dataframe.to_csv('{0}ht_scores.csv'.format(config['result_data']), header=True, encoding='utf-8')
            return
        elif not worker_queue.empty():
            print 'Getting dataframe from queue'
            if append_count == 0:
                dataframe = worker_queue.get()
            else:
                dataframe = dataframe.append(worker_queue.get())
            append_count += 1
        if append_count == 1000:
            append_count = 0
            if os.path.isfile('{0}ht_scores.csv'.format(config['result_data'])):
                dataframe.to_csv('{0}ht_scores.csv'.format(config['result_data']), mode='a', header=False, encoding='utf-8')
            else:
                dataframe.to_csv('{0}ht_scores.csv'.format(config['result_data']), header=True, encoding='utf-8')

if __name__ == '__main__':
    # Load the configuration
    config = Parser().parse_config('config/config.conf', 'AWS')
    # Split the files into smaller files, each one containing no more than 500,000 json lines
    # directory = '{0}*.gz'.format(config['flat_data'])
    # file_names = glob.glob(directory)
    # pool = Pool()
    # pool.imap_unordered(split_file, file_names, 1)
    # pool.close()
    # pool.join()
    #
    # # From the split files, create a location file for each
    # print 'Loading rate imputations'
    # cv_rate = cPickle.load(open(config['price_imputation_text_extractor_location'], 'rb'))
    # rf_rate = cPickle.load(open(config['price_imputation_model_location'], 'rb'))
    # print 'Loading age imputations'
    # cv_age = cPickle.load(open(config['age_imputation_text_extractor_location'], 'rb'))
    # rf_age = cPickle.load(open(config['age_imputation_model_location'], 'rb'))
    #
    # directory = '{0}*.gz'.format(config['split_file_directory'])
    # file_names = glob.glob(directory)
    # pool = Pool()
    # pool.imap_unordered(create_location_files, file_names, 1)
    # pool.close()
    # pool.join()
    #
    # # Merge all of the files together
    # base_list = get_unique_base_file_names('{0}*.csv'.format(config['location_data']))
    # pool = Pool()
    # pool.imap_unordered(merge_files, base_list, 100)
    # pool.close()
    # pool.join()
    #
    # # Calculate stats for each location
    # directory = '{0}*.csv'.format(config['location_data_merged'])
    # file_names = glob.glob(directory)
    #
    # lock = Lock()
    # pool = Pool(initializer=initializeLock, initargs=(lock,))
    # pool.imap_unordered(make_location_stats, file_names)
    # pool.close()
    # pool.join()
    #
    # # Now that we have the location data. Let's calculate stats for each ad.
    # # Since the same ads are in the city and state file, let's only pull from the city files
    # directory = '{0}city*.csv'.format(config['location_data'])
    # file_names = glob.glob(directory)
    #
    # lock = Lock()
    # pool = Pool(initializer=initializeLock, initargs=(lock,))
    # pool.imap_unordered(make_ad_stats, file_names)
    # pool.close()
    # pool.join()
    #
    # # Calculate phone stats
    # lock = Lock()
    # directory = '{0}city*.csv'.format(config['location_data'])
    # file_names = glob.glob(directory)
    # pool = Pool(initializer=initializeLock, initargs=(lock,), processes=3)
    # pool.imap_unordered(make_entity_stats, file_names)
    # pool.close()
    # pool.join()
    #
    # # Now we need the human traficking scores. First get all of the phone numbers in one file
    # chunksize = 100000
    # file_name = '{0}ad_characteristics.csv'.format(config['result_data'])
    # lock = Lock()
    # pool = Pool(initializer=initializeLock, initargs=(lock,))
    # reader = pandas.read_csv(file_name,
    #                          chunksize=chunksize,
    #                          usecols=['phone',
    #                                   'imputed_rate',
    #                                   'imputed_age'])
    #
    # for chunk in reader:
    #     pool.apply_async(create_phone_files, [chunk])
    #
    # pool.close()
    # pool.join()
    #
    #
    # # Next we need to calculate the human traficking scores
    # worker_queue = Queue()
    # ender_queue = Queue()
    #
    # ht_append_process = Process(target=append_ht_scores, args=(worker_queue, ender_queue,))
    # ht_append_process.start()
    # lock = Lock()
    # # Load the ht model
    # pipeline = cPickle.load(open(config['ht_score_model'], 'rb'))
    # directory = '{0}*.csv'.format(config['phone_data'])
    # file_names = glob.glob(directory)
    # pool = Pool(initializer=initializeLock, initargs=(lock,))
    # pool.imap_unordered(get_ht_score, file_names)
    # pool.close()
    # pool.join()
    # ender_queue.put(True)
    # ht_append_process.join()

    # Finally apply the human traficking scores
    chunksize = 100000
    lock = Lock()
    pool = Pool(initializer=initializeLock, initargs=(lock,), processes=1)
    reader = pandas.read_csv('{0}ad_characteristics.csv'.format(config['result_data']),
                             chunksize=chunksize, index_col=0)

    for chunk in reader:
        pool.apply_async(apply_ht_scores, [chunk])

    pool.close()
    pool.join()


    worker_queue = Queue()
    ender_queue = Queue()
    lock = Lock()