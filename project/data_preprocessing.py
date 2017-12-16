import pandas as pd
import numpy as np
import gzip
from helpers import loadCountData, statistics_data, count_review, get_user, get_product
DATA_DIR = 'data/'

'''
This file handles all the data preparation and storage needed for the analysis.
'''

##### Functions for reading and parsing files #####


def parse(path):
    '''
    Parse a gzip file
    '''
    g = gzip.open(path, 'rb')
    for l in g:
        yield eval(l)


def getDF(path):
    '''
    Load a file into a DataFrame
    '''
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
    return pd.DataFrame.from_dict(df, orient='index')

##### Functions related to the DataFrames directly #####


def get_categories(item):
    '''
    Get the category attributes of an item row
    '''
    for cats in item['categories']:
        for cat in cats:
            yield cat


def create_categories_count_df(reviews_df, meta_df):
    '''
    Merge both reviews and meta data into a single Dataframe
    '''
    merged_df = pd.merge(
        meta_df[['asin', 'categories']], reviews_df[['asin']], on='asin')
    categories = {}
    for idx, item in merged_df.iterrows():
        for cat in get_categories(item):
            if cat in categories:
                categories[cat] += 1
            else:
                categories[cat] = 1

    count_series = pd.Series(categories, name='count')
    count_series.index.name = 'category'
    count_series.reset_index()
    count_df = count_series.to_frame().sort_values('count', ascending=False)

    return count_df


def df_with_datetime(df, col_name='datetime', out_format=None):
    '''
    Add a column with the time as datetime
    '''
    if out_format:
        df[col_name] = pd.to_datetime(
            df['unixReviewTime'], unit='s').dt.strftime(out_format)
    else:
        df[col_name] = pd.to_datetime(df['unixReviewTime'], unit='s')

    return df


# CONSTANTS
METADATA_TO_KEEP = ['asin', 'title', 'categories', 'price']
REVIEWS_DATA_TO_KEEP = ['reviewerID', 'asin', 'overall', 'datetime']
HEALTHY_FOOD_KEYWORDS = ['organic', 'natural', 'sugar-free', 'healthy', 'vitamin',
                         'supplement', 'minerals', 'diet', 'vegan']
HEALTHY_SPORT_CATEGORIES = ['Exercise & Fitness', 'Cycling', 'Sport Watches', 'Team Sports',
                            'Strength Training Equipment', 'Action Sports', 'Cardio Training',
                            'Running']
REVIEWS_GROWTH = 'reviews_count_df'
USER_COUNT = 'users_count_df'
PRODUCT_COUNT = 'products_count_df'

##### Functions related to the 'healthiness' of items #####


def is_food_healthy(item):
    '''
    Check if a food item is healthy
    '''
    for kw in HEALTHY_FOOD_KEYWORDS:
        try:
            if kw in item['title'].lower() or kw in item['description'].lower():
                return True
        except:
            pass

    return False


def is_sport_item_healthy(item):
    '''
    Check if a sport item is healthy
    '''
    for cat in get_categories(item):
        if cat in HEALTHY_SPORT_CATEGORIES:
            return True

    return False


def save_data(filename, pickle_filename, with_datetime=False):
    '''
    Load a file into a DataFrame and save its pickle
    '''
    print('Saving file:', DATA_DIR + pickle_filename)

    df = getDF(DATA_DIR + filename)

    if with_datetime:
        df = df_with_datetime(df)

    df.to_pickle(DATA_DIR + pickle_filename)


def save_healthy_data(reviews_filename, metadata_filename, filtering_func, filename):
    '''
    Read, filter and merge healthy products into a Dataframe and then save it to a pickle
    '''
    print('Retrieving data from pickles...')
    reviews_df = pd.read_pickle(DATA_DIR + reviews_filename)
    meta_df = pd.read_pickle(DATA_DIR + metadata_filename)

    # Metadata about healthy products only
    print('Filtering healthy products...')
    meta_healthy_df = meta_df[meta_df.apply(
        lambda item: filtering_func(item), axis=1)]

    # Reviews about healthy products merged with corresponding metadata
    print('Merging dataframes...')
    merged_healthy_df = pd.merge(
        meta_healthy_df[METADATA_TO_KEEP], reviews_df[REVIEWS_DATA_TO_KEEP], on='asin')

    # Store file into a picke
    print('Saving into pickle at:', DATA_DIR + filename)
    merged_healthy_df.to_pickle(DATA_DIR + filename)

    print('Done')


def main():
    '''
    Data preprocessing : generate needed files for Report
    '''
    save_data('reviews_Grocery_and_Gourmet_Food.json.gz',
              'food_reviews_df', True)
    save_data('meta_Grocery_and_Gourmet_Food.json.gz', 'food_meta_df')
    save_data('reviews_Sports_and_Outdoors.json.gz', 'sports_reviews_df', True)
    save_data('meta_Sports_and_Outdoors.json.gz', 'sports_meta_df')
    save_healthy_data(
        'food_reviews_df',
        'food_meta_df',
        is_food_healthy,
        'healthy_food_df'
    )
    save_healthy_data(
        'sports_reviews_df',
        'sports_meta_df',
        is_sport_item_healthy,
        'healthy_sports_df'
    )

    food_reviews_df = pd.read_pickle(DATA_DIR + 'food_reviews_df')
    food_meta_df = pd.read_pickle(DATA_DIR + 'food_meta_df')

    food_cat_count_df = create_categories_count_df(
        food_reviews_df, food_meta_df)
    food_cat_count_df.to_pickle(DATA_DIR + 'food_cat_count_df')

    sports_reviews_df = pd.read_pickle(DATA_DIR + 'sports_reviews_df')
    sports_meta_df = pd.read_pickle(DATA_DIR + 'sports_meta_df')

    sports_cat_count_df = create_categories_count_df(
        sports_reviews_df, sports_meta_df)
    sports_cat_count_df.to_pickle(DATA_DIR + 'sports_cat_count_df')

    loadCountData(REVIEWS_GROWTH, count_review, extra_handling='Reviews')
    loadCountData(USER_COUNT, statistics_data, get_user, 'Users')
    loadCountData(PRODUCT_COUNT, statistics_data, get_product, 'Products')


if __name__ == "__main__":
    main()
