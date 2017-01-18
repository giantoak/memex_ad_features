import pandas

def make_location(file):
    dataframe = pandas.read_csv(file)

    if (len(dataframe) > 100000):
        dataframe = dataframe.sample(n=100000)

