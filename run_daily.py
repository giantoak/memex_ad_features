from config_parser import Parser
from multiprocessing import Pool, Lock
from lattice_json_tools import gzipped_jsonline_file_to_df
from make_ad import MakeAd
import glob
import gzip
import os
import cPickle
import pandas
import re
import datetime

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


def calculate_daily_ad(file):
    """

    :param file:
    :return:
    """
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


    print 'Imputations done for {0}'.format(file)

    # We no longer need the content, so drop it.
    dataframe.drop('content', inplace=True, axis=1)

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

    print '*******************Finished file {0}********************************'.format(file)


def apply_ht_scores(dataframe):
    # Load the ht score dataframe
    ht_scores = pandas.read_csv('{0}ht_scores.csv'.format(config['result_data']), index_col=0)
    dataframe['phone'] = dataframe['phone'].map(lambda x: re.sub('[^0-9]', '', str(x)))
    # Make the column a numeric column for merging
    dataframe['phone'] = pandas.to_numeric(dataframe['phone'])
    final = dataframe.merge(ht_scores, how='left', left_on='phone', right_index=True)

    if os.path.isfile('{0}ad_chars_final.csv'.format(config['result_data'])):
        lock.acquire()
        print 'lock has been set for file {0}'.format(file)
        final.to_csv('{0}ad_chars_final.csv'.format(config['result_data']), mode='a', header=False, encoding='utf-8')
        lock.release()
        print 'lock has been released for file {0}'.format(file)
    else:
        final.to_csv('{0}ad_chars_final.csv'.format(config['result_data']), header=True, encoding='utf-8')


def initializeLock(l):
    """

    :param l: Lock
    :return:
    """
    # Lock needs to be global for it to be passed to map
    global lock
    lock = l


if __name__ == '__main__':
    # Load the configuration
    config = Parser().parse_config('config/config.conf', 'Test')

    # Get only the files with a timestamp of today


    # Split the files into smaller files, each one containing no more than 500,000 json lines
    directory = '{0}*.gz'.format(config['flat_data'])
    file_names = glob.glob(directory)

    today_files = []
    today = datetime.datetime.now()

    for file_name in file_names:
        file_date = datetime.datetime.fromtimestamp(os.path.getmtime(file_name))
        if file_date.day == today.day and file_date.month == today.month and file_date.year == today.year:
            today_files.append(file_name)

    pool = Pool()
    pool.imap_unordered(split_file, today_files, 1)
    pool.close()
    pool.join()

    print 'Loading rate imputations'
    cv_rate = cPickle.load(open(config['price_imputation_text_extractor_location'], 'rb'))
    rf_rate = cPickle.load(open(config['price_imputation_model_location'], 'rb'))
    print 'Loading age imputations'
    cv_age = cPickle.load(open(config['age_imputation_text_extractor_location'], 'rb'))
    rf_age = cPickle.load(open(config['age_imputation_model_location'], 'rb'))

    # Using tthe split files, calculate the ad features
    lock = Lock()
    directory = '{0}*.gz'.format(config['split_file_directory'])
    file_names = glob.glob(directory)
    pool = Pool(initializer=initializeLock, initargs=(lock,))
    pool.imap_unordered(calculate_daily_ad, file_names, 1)
    pool.close()
    pool.join()

    # Finally apply the human traficking scores
    chunksize = 10000
    pool = Pool(initializer=initializeLock, initargs=(lock,))
    reader = pandas.read_csv('{0}ad_characteristics.csv'.format(config['result_data']),
                             chunksize=chunksize, index_col=0)

    for chunk in reader:
        pool.apply_async(apply_ht_scores, [chunk])

    pool.close()
    pool.join()
