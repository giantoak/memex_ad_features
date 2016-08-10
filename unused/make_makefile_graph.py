"""
Program for graphing Makefile dependencies.
"""
color_dict = {
    'tsv': '#1FFF84',
    'csv': '#367F57',
    'txt': '#19CC69',
    'py': '#3CADFF',
    'R': '#89CDFF',
    'r': '#89CDFF',
    '': '#BC91FF'
}


def dict_to_graph_var(var_name, feature_dict):
    """

    :param str var_name:
    :param dict feature_dict:
    :return: `str` --
    """
    return '{} [{}];'.format(var_name,
                             ', '.join(['{}="{}"'.format(f, feature_dict[f])
                                        for f in feature_dict]))


def list_to_cluster(node_list, rank=None):
    """

    :param list node_list:
    :param str rank:
    :return: `str` --
    """
    cluster_str = ', '.join(node_list)
    if rank is None:
        return '{ ' + cluster_str + ' }'
    return '{ rank="' + rank + '" ' + cluster_str + ' }'


def main():
    lines = open('Makefile', 'r').readlines()
    target_lines = [x for x in lines if x[0] != '\t' and x.find(':') > 0]

    graph_features = {'label': 'Makefile Target Order',
                      'rankdir': 'LR',
                      'layout': 'dot',
                      'concentrate': 'true'}
    node_features = {'style': 'filled'}

    orphan_nodes = set()
    childless_nodes = set()

    node_dict = {}
    node_color_dict = {}
    target_source_dict = {}

    node_count = 0
    cluster_count = 0

    for line in target_lines:
        target, sources = line.strip().split(':')
        source_list = [x for x in sources.split() if len(x.strip()) > 0]

        for node in [target] + source_list:
            if node not in node_dict:
                suffix = node.split('.')[-1]
                if suffix in color_dict:
                    node_name = 'node_{}'.format(node_count)
                    node_dict[node] = node_name
                    node_count += 1
                    node_color_dict[node] = color_dict[suffix]

                else:
                    node_name = 'cluster_{}'.format(cluster_count)
                    node_dict[node] = node_name
                    cluster_count += 1
                    node_color_dict[node] = color_dict['']

                if node == target:
                    childless_nodes.add(node_dict[node])

        if len(source_list) > 0:
            node_set = set(node_dict[src] for src in source_list)
            target_source_dict[node_dict[target]] = node_set
            childless_nodes.difference_update(node_set)

        else:
            orphan_nodes.add(node_dict[target])

    # Being childless is more important than being an orphan
    orphan_nodes.difference_update(childless_nodes)



    with open('makefile_graph.dot', 'w') as outfile:
        outfile.write('digraph Make_Targets {\n')
        outfile.write('  {}\n'.format(dict_to_graph_var('graph', graph_features)))
        outfile.write('  {}\n'.format(dict_to_graph_var('node', node_features)))
        for node in node_dict:
            outfile.write('  {} [label="{}", fillcolor="{}"];\n'.format(node_dict[node],
                                                                        node,
                                                                        node_color_dict[node]))

        # outfile.write('  {};\n'.format(list_to_cluster(list(orphan_nodes), 'source')))
        outfile.write('  {};\n'.format(list_to_cluster(list(childless_nodes), 'sink')))

        # for target in target_source_dict:
        #     shared_level = list(target_source_dict[target] - orphan_nodes)
        #     if len(shared_level) > 1:
        #         outfile.write('  {};\n'.format(list_to_cluster(shared_level, 'same')))

        for target in target_source_dict:
            if len(target_source_dict[target]) > 1:
                outfile.write('  {} -> {};\n'.format(
                    list_to_cluster(target_source_dict[target].difference(orphan_nodes)), target))
            else:
                outfile.write('  {} -> {};\n'.format(list(target_source_dict[target])[0],
                                                     target))

        outfile.write('}\n')


if __name__ == "__main__":
    main()
