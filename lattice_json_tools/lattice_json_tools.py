def parse_lattice_json_line(line):
    """
    Parse a line of lattice JSON, trimming out non-lattice fields and flattening nested fields.
    :param str line:
    :returns: `dict` -- parsed and shrunk line of lattice JSON.
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
            # name, wikidata_id, lat, and lon
            context_dict = defaultdict(list)

            for result in jsn['extractions']['lattice-location']['results']:
                context_data = result['context']

                for c_type in context_data:

                    # skip contexts that aren't city or state
                    # per discussion w/ Gabriel & Jeff about which contexts matter.
                    if c_type not in ['city', 'state']:
                        continue

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
    import gzip
    import ujson as json

    outfile = NamedTemporaryFile(mode='w', delete=False)
    with gzip.open(gzip_fpath) as infile:
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
