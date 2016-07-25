def _get_temp_dir():
    import tempfile
    return tempfile.tempdir()


def _get_temp_fpath():
    from tempfile import NamedTemporaryFile
    tmp_file = NamedTemporaryFile(delete=False)
    tmp_name = tmp_file.name
    tmp_file.close()
    return tmp_name


def _validated_snakebite_client(client=None):
    from snakebite.client import Client

    if isinstance(client, Client):
        return client

    return Client('memex-nn2.xdata.data-tactics-corp.com', 8020, use_trash=False)


def _run_copy(copy_file_generator):
    from tqdm import tqdm
    copy_results = []
    for x in tqdm(copy_file_generator):
        copy_results.append(x)
    return copy_results


def copy_hdfs_files_to_local(source_fpaths, dest_dir, client=None):
    """

    :param list source_fpaths:
    :param str dest_dir:
    :param snakebite.Client client:
    :returns: `list` --
    """
    client = _validated_snakebite_client()

    if dest_dir is None:
        dest_dir = _get_temp_dir()

    copy_file_generator = client.copyToLocal(source_fpaths, dest_dir)
    return _run_copy(copy_file_generator)


def copy_hdfs_file_to_local(source_fpath, dest_fpath=None, client=None):
    """

    :param str | list source_fpath:
    :param str dest_fpath:
    :param snakebite.Client client:
    :returns: `dict` --
    """
    if isinstance(source_fpath, str):
        source_fpath = [source_fpath]

    if dest_fpath is None:
        dest_fpath = _get_temp_fpath()

    return copy_hdfs_files_to_local(source_fpath, dest_fpath, client)[0]


def copy_hdfs_dir_to_local(source_dir, dest_dir=None, client=None):
    """

    :param str source_dir:
    :param str dest_dir:
    :param snakebite.Client client:
    :returns: `list` --
    """
    client = _validated_snakebite_client(client)

    src_path_generator = client.ls([source_dir])
    src_paths = [src_dict['path'] for src_dict in src_path_generator]

    return copy_hdfs_files_to_local(src_paths, dest_dir, client)


def copy_local_to_hdfs(source_path, target_path):
    """

    :param str source_path:
    :param str target_path:
    :returns: `bool` --
    """
    try:
        import subprocess
        subprocess.check_call(['hdfs', 'dfs', '-put', source_path, target_path], shell=False)
    except:
        return False
    return True


def copy_month_of_lattice_extracts(year_month_str, target_dir=None):
    return copy_hdfs_files_to_local(
        ['/user/lattice/extract/{}/complete/*.json.gz'.format(year_month_str)],
        target_dir)


def copy_all_lattice_extracts(target_dir=None):
    return copy_hdfs_files_to_local(
        ['/user/lattice/extract/*/complete/*.json.gz'],
        target_dir)
