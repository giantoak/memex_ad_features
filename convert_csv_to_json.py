import pandas
import glob
from multiprocessing import Pool
from config_parser import Parser


def get_csv_to_json_filaname(filename):
    new_filename = filename[:-3]
    new_filename += 'json'
    return new_filename

def convert_csv_to_json(file):
    print 'Converting {0}'.format(file)
    df = pandas.read_csv(file)
    df.to_json(get_csv_to_json_filaname(file))


if __name__ == '__main__':
    config = Parser().parse_config('config/config.conf', 'AWS')
    glob = glob.glob('{0}*.csv'.format(config['result_data']))

    pool = Pool()
    pool.imap_unordered(convert_csv_to_json, glob)
    pool.close()
    pool.join()