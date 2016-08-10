from create_dataframe import DFManager
from make_msa import MakeMSA
from make_ad import MakeAd
from make_entity import MakeEntity
import pandas as pd
from glob import glob

#  config = {'filenames': glob.glob('/home/ubuntu/2016_summer_camp/classifier/data/initial/lattice/*gz')}
# config = {'filenames': glob('lattice_data/flattened/flattened_before_201605/*.json.gz')[:2]}
config = {'filenames': glob('/home/ubuntu/flat_data/flattened*2016*/*.json.gz')}


def run_location_features(dfm):
    """
    Will get features by city and state
    :param create_dataframe.DFManager dfm:
    """
    df = dfm.create_msa_data_frame()
    make_msa = MakeMSA(df).get_msa_features()
    for loc_type in ['city', 'state']:
        make_msa['{}_stats'.format(loc_type)].to_csv(
            'data/location_characteristics_{}.csv'.format(loc_type),
            sep='\t',
            encoding='utf8')


def run_ad_features(dfm):
    """
    Will run the ad features
    :param create_dataframe.DFManager dfm:
    """
    df = dfm.create_ad_dataframe()

    # Get the city and state features
    city_features = pd.read_table('data/location_characteristics_city.csv')
    state_features = pd.read_table('data/location_characteristics_state.csv')

    make_ad = MakeAd(city_features, state_features, df)
    ad_features = make_ad.get_ad_features()
    ad_features.to_csv('data/ad_characteristics.csv',
                       sep='\t',
                       encoding='utf8',
                       index=False)


def run_entity_features(dfm, entity):
    """

    :param create_dataframe.DFManager dfm:
    :param entity: The entity you'd like to do stats on
    """
    df = dfm.create_entity_dataframe(entity)
    make_entity = MakeEntity(df, entity).get_entity_features()
    make_entity.to_csv('data/{}_characteristics.csv'.format(entity),
                       sep='\t',
                       encoding='utf8',
                       index=False)


def main():

    print('Initializing DF Manager...')
    dfm = DFManager(config)
    print('Done!')

    print('Getting location features...')
    run_location_features(dfm)
    print('Done!')

    print('Getting ad features...')
    run_ad_features(dfm)
    print('Done!')

    print('Getting (phone) entity features...')
    run_entity_features(dfm, 'phone')
    print('Done!')


if __name__ == "__main__":
    main()

