'''
This file contains a bunch of helpers methods for computing statistics and
different DFs on our data.
'''

import os
import gzip
import datetime
import pandas as pd
from sklearn import linear_model
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from tqdm import tqdm_notebook as tqdm

# Constants
REVIEWS_DATE_FORMAT = "%m %d, %Y"
MONT_DATE_FORMAT = '%Y-%m'
REVIEWS_GROWTH = 'reviews_count_df'
USER_COUNT = 'users_count_df'
PRODUCTS_COUNT = 'products_count_df'
DATA_DIR = 'data/'


##### Functions related to the DataFrames directly #####

def truncate_date_df(df, col_name='datetime', from_date='2003-01-01', to_date='2014-07-01'):
    '''
    Remove the rows of a Dataframe to keep only the one in a given time frame
    '''
    return df[(df[col_name] >= from_date) & (df[col_name] < to_date)]


##### Functions to compute some statistics

def get_date(item):
    '''
    Retrieves the date at what time a given review is done
    '''
    try:
        return datetime.datetime.fromtimestamp(item['unixReviewTime'])
    except KeyError:
        return datetime.datetime.strptime(item['reviewTime'], REVIEWS_DATE_FORMAT)


def convert_date_to_month(date):
    '''
    Map a datetime to his month
    '''
    return datetime.datetime.strptime(date.strftime(MONT_DATE_FORMAT), MONT_DATE_FORMAT)


def month_year_to_date(date_str):
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
    # iterate over the file
    for l in g:
        row = eval(l)
        try:
            date_key = month_year_to_date(get_date(row).strftime('%Y-%m'))
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


def load_count_data(filename, count_func, get_item=None, extra_handling=None, truncate=True):
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
            if file.startswith('reviews') and file.endswith('.json.gz'):
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
            active_df = pd.DataFrame(list(acc_active.items()),
                                     columns=['datetime', 'Active'])

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
        df = df.loc[(df.index.get_level_values('Year') >= 2003) &
                    ((df.index.get_level_values('Year') < 2014) |
                     ((df.index.get_level_values('Year') == 2014)
                      & (df.index.get_level_values('Month') < 7))
                    )]

    return df


def get_user(item):
    '''
    Retrieves the ReviewID for a given review
    '''
    try:
        reviewer_id = item['reviewerID']
    except:
        reviewer_id = None
    return reviewer_id


def statistics_data(file_path, acc_new, acc_active, get_data):
    '''
    Find the statistics about new/active user (or product) in a given file
    and update the values in an accumulator
    '''
    skipoed = 0
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
            date_key = month_year_to_date(get_date(row).strftime('%Y-%m'))
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
            skipped += 1
            continue
    if skipped:
        print('skipped {} rows because of KeyError (not present) or ValueError (not parsable)'.format(skipped))


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
    '''
    add the active count of item to the statistic DataFrame
    '''
    # Count the number of different item for a given Year and month
    item_df = item_df.groupby([item_df.datetime.dt.year, item_df.datetime.dt.month])[column_name].nunique()
    # Store values
    stat_df['Active'] = item_df
    return stat_df.dropna()


def add_launch(item_df, reviews_df):
    '''
    Add the launch date to an item Dataframe given its reviews DataFrame
    '''
    temp_df = reviews_df.copy().sort_values('datetime')
    temp_df = temp_df.groupby('asin').first()
    item_df = item_df.set_index('asin')
    item_df['datetime'] = temp_df.datetime
    return item_df.reset_index()


def get_trend(reviews_df, column, products_df, category='trend', from_year=2003):
    '''
    Compute the trend given statistics of reviews, reviewers and products
    '''
    # Compute trend
    trend_df = reviews_df[[column]].copy()
    trend_df[column] = trend_df[column] / products_df.Active

    # Resetting index
    trend_df = trend_df.reset_index(0)
    trend_df.columns = ['year', column]
    trend_df = trend_df.reset_index()
    trend_df.columns = ['month', 'year', column]

    # Truncate
    trend_df = trend_df[trend_df.year >= from_year].set_index(
        ['year', 'month']).rename(columns={column: category})
    return trend_df


def get_products_stat(reviews_df, meta_df):
    '''
    Compute the products' statistics given reviews and meta DataFrame
    '''
    products_count = meta_df.groupby([meta_df.datetime.dt.year,
                                      meta_df.datetime.dt.month])[['asin']].nunique()
    products_count = products_count.rename(columns={'asin': 'New'})
    products_count['Total'] = products_count.New.cumsum()
    products_count = add_active(products_count, reviews_df, 'asin')
    return products_count


def get_reviewers_stat(reviews_df):
    '''
    Compute the reviewers' statistics given reviews DataFrame
    '''
    reviewers_count = reviews_df[['reviewerID', 'datetime']]
    reviewers_count = reviewers_count.sort_values('datetime')
    # Get first reviews of all reviewers
    reviewers_count = reviewers_count.groupby(
        'reviewerID').first().reset_index()
    # Group by Year and Month
    reviewers_count = reviewers_count.groupby([reviewers_count.datetime.dt.year,
                                               reviewers_count.datetime.dt.month]).count()[['reviewerID']]

    # Compute some more statistics
    reviewers_count = reviewers_count.rename(columns={'reviewerID': 'New'})
    reviewers_count['Total'] = reviewers_count.New.cumsum()
    reviewers_count = add_active(reviewers_count, reviews_df, 'reviewerID')
    return reviewers_count


def get_reviews_stat(reviews_df):
    '''
    Compute the reviews' statistics given reviews DataFrame
    '''

    reviews_df = reviews_df.groupby([reviews_df.datetime.dt.year, reviews_df.datetime.dt.month]).count()
    return reviews_df


def get_normalized_date(df):
    '''
    Transform a all Dates (Month and Year) from a DF to the number of years (in decimal) from
    the earliest Date present in the DF
    '''
    return df['year'] + (df['month'] - df['month'].min()) / 12 - df['year'].min()


def get_ratio_trend_and_estimation(healthy_trend, food_and_sport_trend, healthy_col_name="Healthy", food_and_sport_col_name="Food and Sport"):
    '''
    Get a DF containing the ratio between values in a certain column from 2 similar DFs,
    compute a regression (using Ridge regression with PolynomialFeature of degree 4) on
    the ratio using the normalized date as feature and add it to the returned DF
    '''

    # Rename columns to the same name in both DFs
    healthy_df = healthy_trend.copy()
    healthy_df = healthy_df.rename(columns={healthy_col_name: "Total"})
    food_and_sport_df = food_and_sport_trend.copy()
    food_and_sport_df = food_and_sport_df.rename(columns={food_and_sport_col_name: "Total"})

    # Compute the diff
    ratio_df = healthy_df / food_and_sport_df
    ratio_df = ratio_df.rename(columns={"Total": "Ratio"})
    ratio_df = ratio_df.reset_index().dropna()
    ratio_df['NormalizedDate'] = get_normalized_date(ratio_df)

    # Create model with Ridge regression
    model = make_pipeline(PolynomialFeatures(4), linear_model.Ridge())
    model.fit(ratio_df[['NormalizedDate']].values, ratio_df[['Ratio']].values.flatten())
    ratio_df['Regression'] = model.predict(ratio_df['NormalizedDate'].values.reshape(-1, 1))

    return ratio_df.set_index(['year', 'month'])[['Regression', 'Ratio']]
