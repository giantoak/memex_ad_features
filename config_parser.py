import ConfigParser


class Parser():

    def __init__ (self):
        pass

    def parse_config(self, filename, section='Test'):
        """
        :param filename: Location of configs
        :param section: Section to parse. Defaults to 'test
        :return: A dictionary of the specificied configurations
        """
        config = ConfigParser.ConfigParser()
        config.read(filename)
        dict1 = {}
        options = config.options(section)
        for option in options:
            try:
                dict1[option] = config.get(section, option)
                if dict1[option] == -1:
                    pass
            except:
                print("exception on %s!" % option)
                dict1[option] = None
        return dict1