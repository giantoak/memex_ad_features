import ujson
import pandas
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import FeatureUnion
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier

out_file_name = 'cp1_subset'


class ItemSelector(BaseEstimator, TransformerMixin):
    """For data grouped by feature, select subset of data at a provided key.
    The data is expected to be stored in a 2D data structure, where the first
    index is over features and the second is over samples.  i.e.
    >> len(data[key]) == n_samples
    Please note that this is the opposite convention to scikit-learn feature
    matrixes (where the first index corresponds to sample).
    ItemSelector only requires that the collection implement getitem
    (data[key]).  Examples include: a dict of lists, 2D numpy array, Pandas
    DataFrame, numpy record array, etc.
    >> data = {'a': [1, 5, 2, 5, 2, 8],
               'b': [9, 4, 1, 4, 1, 3]}
    >> ds = ItemSelector(key='a')
    >> data['a'] == ds.transform(data)
    ItemSelector is not designed to handle data grouped by sample.  (e.g. a
    list of dicts).  If your data is structured this way, consider a
    transformer along the lines of `sklearn.feature_extraction.DictVectorizer`.
    Parameters
    ----------
    key : hashable, required
        The key corresponding to the desired value in a mappable.
    """

    def __init__(self, keylist):
        self.keylist = keylist

    def fit(self, x, y=None):
        self._feature_names = x.columns
        return self

    def get_feature_names(self):
        return (self._feature_names)

    def transform(self, data_dict):
        return data_dict[self.keylist]


class GroupbyMax(BaseEstimator, TransformerMixin):
    def __init__(self, grouping_column=None, max_column=None):
        self.grouping_column = grouping_column
        self.max_column = max_column

    def fit(self, x, y=None):
        # self._feature_names = x.columns
        return self

    def transform(self, data):
        maxes = data.groupby(self.grouping_column)[self.max_column].max()
        return (maxes)


class Uniquifier(BaseEstimator, TransformerMixin):
    def __init__(self, grouping_column=None):
        self.grouping_column = grouping_column

    def fit(self, x, y=None):
        # self._feature_names = x.columns
        return self

    def transform(self, data):
        out = data.groupby(self.grouping_column[0]).size()
        # ipdb.set_trace()
        return (pandas.DataFrame(out.index))


class Summarizer(BaseEstimator, TransformerMixin):
    '''
    initialized with a grouping column where we do a groupb_by max of matches
    Calls to .transform grab only the named column and transform them to dummies
    '''

    def __init__(self, grouping_column=None):
        self.grouping_column = grouping_column

    def fit(self, x, y=None):
        # self._feature_names = x.columns
        return self
        # def get_feature_names(self):
        # return(self._feature_names)

    def transform(self, data):
        main_column = [i for i in data.columns if i != self.grouping_column][0]
        summary_stats = data.groupby(self.grouping_column)[main_column].describe().unstack()
        del summary_stats['count']
        summary_stats = summary_stats.rename(columns={i: '%s__%s' % (main_column, i) for i in summary_stats.columns})
        summary_stats = summary_stats.fillna(-1)
        return summary_stats


