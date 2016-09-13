def parse_location_contexts(jsn):
    """
    A given CDR ID can have multiple location contexts
    Each context can have up to four components: city, country, state, location
    At present, we only care about city & state
    Each component has four descriptors: name, wikidata_id, lat, and lon
    :param dict jsn: <lattice_entry>['extractions']['lattice-location']
    :returns: `list` --
    """
    flat_contexts = []
    for context_dict in jsn['results']:
        context_data = context_dict['context']
        flat_context = {}
        for c_type in context_data:

            # At present, we only care about city and state contexts
            if c_type not in ['city', 'state']:
                continue

            # Skip missing entries
            if context_data[c_type]['name'] is None:
                continue

            for key in ['name', 'wikidata_id']:
                flat_context['location_{}_{}'.format(c_type, key)] = \
                    context_dict['context'][c_type][key]

            flat_context['location_{}_lat_lon'.format(c_type)] = \
                (context_data[c_type]['centroid_lat'],
                 context_data[c_type]['centroid_lon'])

        flat_contexts.append(flat_context)

    return flat_contexts


def parse_lattice_jsonline(line):
    """
    Parse a line of lattice JSON, trimming out non-lattice fields and flattening nested fields.
    Location contexs are flattened in bulk, preserving pairings.
    :param str line:
    :returns: `list` -- list of "unpacked" JSON dicts: values are not lists but rather single entries.
    """
    import ujson as json
    from collections import defaultdict

    jsn = json.loads(line)
    new_jsn_dict = {'_id': jsn['_id']}
    l_keys = [key for key in jsn['extractions'] if key.find('lattice-') == 0]

    for key in l_keys:

        if key == 'lattice-location':
            # We solve this key below.
            continue

        if isinstance(jsn['extractions'][key]['results'], list):
            values = [x['value'] for x in jsn['extractions'][key]['results']
                      if 'value' in x]
            if len(values) == 1:
                values = values[0]
        elif isinstance(jsn['extractions'][key]['results'], dict):
            values = jsn['extractions'][key]['results']['value']
        else:
            # we ignore all items without an explicit 'value' key
            pass

        new_jsn_dict[key[8:]] = values

    # Create a JSON for each location context
    if 'lattice-location' in l_keys:
        flat_contexts = parse_location_contexts(jsn['extractions']['lattice-location'])
        jsn_dict_list = []
        for flat_context in flat_contexts:
            jsn_dict_list.append(new_jsn_dict.copy())
            for key in flat_context:
                jsn_dict_list[-1][key] = flat_context[key]
    else:
        jsn_dict_list = [new_jsn_dict]

    # Create new JSON dicts for fields with multiple values
    i = 0
    while i < len(jsn_dict_list):
        for key in jsn_dict_list[i]:
            if isinstance(jsn_dict_list[i][key], list):
                for value in jsn_dict_list[i][key][1:]:
                    jsn_copy = jsn_dict_list[i].copy()
                    jsn_copy[key] = value
                    jsn_dict_list.append(jsn_copy)
                jsn_dict_list[i][key] = jsn_dict_list[i][key][0]
        i += 1

    return jsn_dict_list


def convert_file_to_gzip(in_fpath, remove_old=True):
    import gzip
    import shutil
    out_fpath = '{}.json.gz'.format(in_fpath)
    with open(in_fpath, 'rb') as infile:
        with gzip.open(out_fpath, 'wb') as outfile:
            shutil.copyfileobj(infile, outfile)

    if remove_old:
        from os import remove
        remove(in_fpath)

    return out_fpath


def stream_lattice_jsonline_gzip_file_to_file(gzip_fpath,
                                              gzip_output=True,
                                              remove_unzipped=True):
    """
    Read through a gzip of lattice JSON, parsing it to a tempfile;
    :param str gzip_fpath: Path to gzip file
    :param bool gzip_output:
    :param bool remove_unzipped:
    :returns: `str` -- Path to tempfile of shrunk json
    """

    from tempfile import NamedTemporaryFile
    import gzip
    import ujson as json

    outfile = NamedTemporaryFile(mode='w', delete=False)
    with gzip.open(gzip_fpath) as infile:
        for line in infile:
            jsn_list = parse_lattice_jsonline(line)
            for jsn in jsn_list:
                outfile.write('{}\n'.format(json.dumps(jsn)))
    outfile.close()

    if gzip_output is True:
        return convert_file_to_gzip(outfile.name, remove_unzipped)

    return outfile.name


def validate_hdfs_copy_and_parse(copy_file_dict):
    """
    If HDFS copy was successful, parse content to file and return path
    :param dict copy_file_dict: dict of file copy info from snakebite.copyToLocal
    :returns: `None` or `str` -- path to parsed file of json lines or None
    """
    if copy_file_dict['result'] is False:
        return None

    return stream_lattice_jsonline_gzip_file_to_file(copy_file_dict['path'])
