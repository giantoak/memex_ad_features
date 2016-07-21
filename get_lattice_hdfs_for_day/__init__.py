def parse_lattice_json_line(line):
    """

    :param str|unicode line:
    :returns: `str` -- parsed and s
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
            # Each context has foour identifiers:
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

    return json.dumps(new_jsn_dict)


def parse_lattice_json_gzip_file(gzip_fpath):
    """

    :param str|unicode gzip_fpath:
    :returns: `str` -- path to temporary file
    """
    from tempfile import NamedTemporaryFile
    from gzip import GzipFile

    outfile = NamedTemporaryFile(delete=False)
    with GzipFile(gzip_fpath) as infile:
        for line in infile:
            outfile.write(parse_lattice_json_line(line) + '\n')
    outfile.close()

    return outfile.name


def validate_hdfs_copy_and_parse(copy_file_dict):
    """

    :param dict copy_file_dict:
    :returns: `None` or `tempfile.NamedTemporaryFile` -- path to parsed file of json lines
    """
    if copy_file_dict['result'] is False:
        return None
    return parse_lattice_json_gzip_file(copy_file_dict['path'])


def get_lattice_hdfs_for_day(day_str, client=None):
    """

    :param str day_str: date in YYYYMMDD format
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

    src_path_generator = client.ls(['/user/lattice/extract/{}/complete'.format(day_str)])
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
