def parse_lattice_json_line(line):
    """
    Parse a line of lattice JSON, trimming out non-lattice fields and flattening nested fields.
    :param str line:
    :returns: `str` -- parsed and shrunk line of lattice JSON.
    """
    import ujson as json
    from collections import defaultdict

    jsn = json.loads(line)
    new_jsn_dict = {'_id': jsn['_id']}
    lattice_keys = [key for key in jsn['extractions'] if key.find('lattice-') == 0]

    for key in lattice_keys:
        if key == 'lattice-location':

            # Each result in lattice-location has
            # four contexts:
            # city, country, state, and location
            # Each context has four identifiers:
            # name, wikidiata_id, lat, and lon
            context_dict = defaultdict(list)

            for result in jsn['extractions']['lattice-location']['results']:
                context_data = result['context']

                for c_type in context_data:
                    # skip missing entries
                    if context_data[c_type]['name'] is None:
                        continue

                    # skip contexts we've already seen for this result.
                    if context_data[c_type]['name'] in context_dict['{}_name'.format(c_type)]:
                        continue

                    context_dict['{}_name'.format(c_type)].append(context_data[c_type]['name'])
                    context_dict['{}_wikidata_id'.format(c_type)].append(context_data[c_type]['wikidata_id'])
                    context_dict['{}_lat_lon'.format(c_type)].append((context_data[c_type]['centroid_lat'],
                                                                      context_data[c_type]['centroid_lon']))

            for key in context_dict:
                new_jsn_dict['location_{}'.format(key)] = context_dict[key]

        else:
            value_list = [x['value'] for x in jsn['extractions'][key]['results']
                          if 'value' in x]
            if key in ['lattice-content', 'lattice-crawltime', 'lattice-title']:
                new_jsn_dict[key[8:]] = value_list[0]
            else:
                new_jsn_dict[key[8:]] = value_list

    return new_jsn_dict


def stream_lattice_json_gzip_file_to_file(gzip_fpath):
    """
    Read through a gzip of lattice JSON, parsing it to a tempfile;
    :param str gzip_fpath: Path to gzip file
    :returns: `str` -- Path to tempfile of shrunk json
    """

    from tempfile import NamedTemporaryFile
    from gzip import GzipFile
    import ujson as json

    outfile = NamedTemporaryFile(delete=False)
    with GzipFile(gzip_fpath) as infile:
        for line in infile:
            outfile.write(json.dumps(parse_lattice_json_line(line)) + '\n')
    outfile.close()

    return outfile.name


def validate_hdfs_copy_and_parse(copy_file_dict):
    """
    If HDFS copy was successful, parse content to file and return path
    :param dict copy_file_dict: dict of file copy info from snakebite.copyToLocal
    :returns: `None` or `str` -- path to parsed file of json lines or None
    """
    if copy_file_dict['result'] is False:
        return None

    return stream_lattice_json_gzip_file_to_file(copy_file_dict['path'])


def get_lattice_hdfs_for_month(month_str, client=None):
    """
    Copy directory of lattice data over to
    :param str month_str: date in YYYYMM format
    :param snakebite.client.Client client:
    :return: `pandas.DataFrame` --
    """
    import logging
    import multiprocessing as mp
    import pandas as pd
    import tempfile
    import ujson as json

    logging.basicConfig()
    logger = logging.getLogger('process_day_folder_to_df')
    logger.setLevel(logging.INFO)

    if client is None:
        from snakebite.client import Client
        client = Client('memex-nn2.xdata.data-tactics-corp.com', 8020, use_trash=False)

    src_path_generator = client.ls(['/user/lattice/extract/{}/complete'.format(month_str)])
    src_paths = [src_dict['path'] for src_dict in src_path_generator]
    logger.info('{} source paths in directory'.format(len(src_paths)))

    src_paths = src_paths[:2]
    logger.info('Limiting to {} for test'.format(len(src_paths)))

    temp_folder = tempfile.gettempdir()
    copy_file_generator = client.copyToLocal(src_paths, temp_folder)
    dfs = []

    logger.info('Starting pool.')
    pool = mp.Pool(min(5, len(src_paths)))
    for result_fpath in pool.imap_unordered(validate_hdfs_copy_and_parse, copy_file_generator):
        if result_fpath is None:
            logger.info('Got None.')
            continue
        logger.info('Reading {} into DataFrame'.format(result_fpath))
        dfs.append(pd.DataFrame.from_records([json.loads(x) for x in open(result_fpath, 'r')]))
    pool.close()
    pool.join()
    logger.info('Pool closed.')

    return pd.concat(dfs)
