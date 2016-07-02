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


def main():
    lines = open('Makefile', 'r').readlines()
    target_lines = [x for x in lines if x[0] != '\t' and x.find(':') > 0]

    edge_list = []

    node_dict = {}
    node_color_dict = {}
    pos_dict = {}

    node_count = 0

    for line in target_lines:
        target, sources = line.strip().split(':')
        source_list = [x for x in sources.split() if len(x.strip()) > 0]

        for node in [target] + source_list:
            if node not in node_dict:
                node_name = 'node_{}'.format(node_count)
                node_dict[node] = node_name
                node_count += 1
                suffix = node.split('.')[-1]
                if suffix in color_dict:
                    node_color_dict[node] = color_dict[suffix]
                else:
                    node_color_dict[node] = color_dict['']

        max_prev = max([0] + [pos_dict[source] for source in source_list if source in pos_dict])
        if target in pos_dict:
            pos_dict[target] = max(max_prev + 1, pos_dict[target])
        else:
            pos_dict[target] = max_prev + 1

        for source in source_list:

            edge_list.append('{} -> {}'.format(node_dict[source], node_dict[target]))

            if source in pos_dict:
                continue

            pos_dict[source] = pos_dict[target] - 1

    with open('makefile_graph.dot', 'w') as outfile:
        outfile.write('digraph Make_Targets {\n')
        outfile.write('graph [label = "Makefile Target Order", rankdir="LR", layout="dot"];\n')
        outfile.write('node [style=filled];\n')
        for node in node_dict:
            outfile.write('  {} [label="{}", fillcolor="{}", rank="{}"];\n'.format(node_dict[node],
                                                                                   node,
                                                                                   node_color_dict[node],
                                                                                   pos_dict[node]))

        for line in edge_list:
            outfile.write('  {};\n'.format(line))
        outfile.write('}\n')


if __name__ == "__main__":
    main()
