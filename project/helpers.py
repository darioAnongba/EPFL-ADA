import os
import gzip
import datetime
import pandas as pd
import numpy as np
import seaborn as sb
from tqdm import tqdm_notebook as tqdm
import matplotlib.pyplot as plt
DATA_DIR = 'data/'

##### Functions related to the DataFrames directly #####


def get_categories(item):
    for cats in item['categories']:
        for cat in cats:
            yield cat


def create_categories_count_df(reviews_df, meta_df):
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


def truncate_date_df(df, col_name='datetime', from_date='2003-01-01', to_date='2014-07-01'):
    return df[(df[col_name] >= from_date) & (df[col_name] < to_date)]

# Functions related to compute some statistics


REVIEWS_DATE_FORMAT = "%m %d, %Y"
MONT_DATE_FORMAT = '%Y-%m'
REVIEWS_GROWTH = 'reviews_count_df'
USER_COUNT = 'users_count_df'
PRODUCTS_COUNT = 'products_count_df'


def getDate(item):
    '''
    Retrieves the date at what time a given review is done
    '''
    try:
        return datetime.datetime.fromtimestamp(item['unixReviewTime'])
    except KeyError:
        return datetime.datetime.strptime(item['reviewTime'], REVIEWS_DATE_FORMAT)


def convert_Date_To_Month(date):
    return datetime.datetime.strptime(date.strftime(MONT_DATE_FORMAT), MONT_DATE_FORMAT)


def MonthYearToDate(date_str):
    '''
    Convert a string date of the form "2016-02" to datetime
    '''
    return datetime.datetime.strptime(date_str, MONT_DATE_FORMAT)


def count_review(file_path, acc, *args):
    '''
    Count the number of reviews in a given file and increase the counts in an accumulator
    '''
    skipped = 0
    g = gzip.open(file_path, 'rb')
    for l in g:
        row = eval(l)
        try:
            date_key = MonthYearToDate(getDate(row).strftime('%Y-%m'))
        except (KeyError, ValueError) as e:
            #print('row is : {}'.format(row))
            skipped += 1
            continue
        if date_key in acc:
            acc[date_key] += 1
        else:
            acc[date_key] = 1
    if skipped:
        print('skipped {} rows because of KeyError (not present) or ValueError (not parsable)'.format(skipped))


def loadCountData(filename, count_func, get_item=None, extra_handling=None, truncate=True):
    '''
    Load a DataFrame of the count of reviews/users
    '''
    # Check if the file was already computed
    if not os.path.isfile(DATA_DIR + filename):
        print('Computing file...')
        acc_new = {}
        acc_active = {}
        # Iterate over the files
        logs = tqdm(os.listdir(DATA_DIR))
        for file in logs:
            logs.set_description(file)
            # Only take reviews files
            if (file.startswith('reviews') and file.endswith('.json.gz')):
                count_func(DATA_DIR + file, acc_new, acc_active, get_item)

        # Special operation to create Dataframe
        if extra_handling == 'Reviews':
            # Convert to DataFrame
            df = pd.DataFrame(list(acc_new.items()),
                              columns=['datetime', 'New'])
            # Use datetime as index
            df = df.groupby([df.datetime.dt.year, df.datetime.dt.month]).sum()
        elif extra_handling in ['Users', 'Products']:
            new_df = pd.DataFrame(list(acc_new.items()),
                                  columns=['New', 'datetime'])
            # Map list to count
            acc_active = {k: len(v) for k, v in acc_active.items()}
            active_df = pd.DataFrame(list(acc_active.items()), columns=[
                                     'datetime', 'Active'])

            new_df = new_df.groupby(
                [new_df.datetime.dt.year, new_df.datetime.dt.month]).count()
            active_df = active_df.groupby(
                [active_df.datetime.dt.year, active_df.datetime.dt.month]).sum()
            df = new_df.join(active_df, how='outer').drop('datetime', axis=1)

        df.index.names = ['Year', 'Month']
        df['Total'] = df.New.cumsum()
        df = df.fillna(0)
        df.to_pickle(DATA_DIR + filename)
    else:
        # Load DataFrame from file
        print('Loading from file...')
        df = pd.read_pickle(DATA_DIR + filename)
    # Truncate to take only relevant time frame
    if truncate:
        df = df.loc[(df.index.get_level_values('Year') > 2003) &
                    (df.index.get_level_values('Year') < 2014)]

    return df


def get_user(item):
    '''
    Retrieves the ReviewID for a given review
    '''
    try:
        reviewerID = item['reviewerID']
    except:
        reviewerID = None
    return reviewerID


def statistics_data(file_path, acc_new, acc_active, get_data):
    '''
    Find the statistics about new/active user (or product) in a given file and update the values in an accumulator
    '''
    skiped = 0
    g = gzip.open(file_path, 'rb')
    for l in g:
        row = eval(l)
        try:
            # Get ReviewerID
            item = get_data(row)
            if item is None:
                # If no item go to next
                continue
            # Get item's date
            date_key = MonthYearToDate(getDate(row).strftime('%Y-%m'))
            # Update new item's accumulator
            if item not in acc_new or item in acc_new and acc_new[item] > date_key:
                acc_new[item] = date_key

            # Update active item's accumulator
            if date_key not in acc_active:
                # Create list for the month
                acc_active[date_key] = set([item])
            else:
                # Check user not already active
                if not item in acc_active[date_key]:
                    acc_active[date_key].add(item)

        except (KeyError, ValueError) as e:
            #print('row is : {}'.format(row))
            skiped += 1
            continue
    if skiped:
        print('skipped {} rows because of KeyError (not present) or ValueError (not parsable)'.format(skiped))


def get_product(product):
    '''
    Retrieves the product ASIN for a given product
    '''
    try:
        asin = product['asin']
    except:
        asin = None
    return asin

def add_active(stat_df, item_df, column_name):
    item_df = item_df.groupby([item_df.datetime.dt.year, item_df.datetime.dt.month])[column_name].nunique()
    stat_df['Active'] = item_df
    return stat_df.dropna()

def add_launch(item_df, reviews_df):
    '''
    add the launch date to an item Dataframe given its reviews DataFrame
    '''
    temp_df = reviews_df.copy().sort_values('datetime')
    temp_df = temp_df.groupby('asin').first()
    item_df = item_df.set_index('asin')
    item_df['datetime'] = temp_df.datetime
    return item_df.reset_index()


def get_trend(reviews_df, column, reviewers_df, products_df, category='trend', from_year=2003):
    trend_df = reviews_df[[column]].copy()
    trend_df[column] = trend_df[column] / \
        products_df.Active / reviewers_df.Active

    # Resetting index
    trend_df = trend_df.reset_index(0)
    trend_df.columns = ['year', column]
    trend_df = trend_df.reset_index()
    trend_df.columns = ['month', 'year', column]

    # Truncate
    trend_df = trend_df[trend_df.year > from_year].set_index(['year', 'month']).rename(columns = {column: category})
    return trend_df
