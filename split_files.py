import gzip
import glob
import os
from config_parser import Parser
from multiprocessing import Lock, Pool

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

if __name__ == '__main__':
    config = Parser().parse_config('config/config.conf', 'AWS')
    directory = '/home/ubuntu/flat_data/data*.gz'
    file_names = glob.glob(directory)

    pool = Pool()
    pool.map(split_file, file_names)
    pool.close()
    pool.join()
