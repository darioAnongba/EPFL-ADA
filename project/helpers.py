import os
import gzip
import datetime
import pandas as pd
import numpy as np
import seaborn as sb
from tqdm import tqdm_notebook as tqdm
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
REVIEWS_GROWTH = 'reviews_count_df'
USER_COUNT = 'users_count_df'


def getDate(item):
    '''
    Retrieves the date at what time a given review is done
    '''
    try:
        return datetime.datetime.fromtimestamp(item['unixReviewTime'])
    except KeyError:
        return datetime.datetime.strptime(item['reviewTime'], REVIEWS_DATE_FORMAT)


def MonthYearToDate(date_str):
    '''
    Convert a string date of the form "2016-02" to datetime
    '''
    return datetime.datetime.strptime(date_str, "%Y-%m")


def count_review(acc, file_path):
    '''
    Count the number of reviews in a given file and increase the counts in an accumulator
    '''
    skipped = 0
    g = gzip.open(file_path, 'rb')
    for l in g:
        row = eval(l)
        try:
            date_key = getDate(row).strftime('%Y-%m')
        except (KeyError, ValueError) as e:
            #print('row is : {}'.format(row))
            skiped += 1
            continue
        if date_key in acc:
            acc[date_key] += 1
        else:
            acc[date_key] = 1
    if skipped:
        print('skipped {} rows because of KeyError (not present) or ValueError (not parsable)'.format(skipped))


def loadCountData(filename, columns, count_func, extra_handling, truncate=False):
    '''
    Load a DataFrame of the count of reviews/users
    '''
    # Check if the file was already computed
    if not os.path.isfile(DATA_DIR + filename):
        print('Computing file...')
        acc = {}
        # Iterate over the files
        logs= tqdm(os.listdir(DATA_DIR))
        for file in logs:
            logs.set_description(file)
            # Only take reviews files
            if (file.startswith('reviews') and file.endswith('.json.gz')):
                count_func(acc, DATA_DIR + file)

        # Convert to DataFrame
        df = pd.DataFrame(list(acc.items()),
                          columns=columns).sort_values('Date')
        # Truncate to take only relevant time frame
        if truncate:
            df = truncate_date_df(df, col_name='Date',
                                  from_date='2003-01-01', to_date='2014-07-01')
        # Special operation
        if extra_handling == 'Reviews':
            # Convert Date to datetime and set it to index
            df.Date = df.Date.map(MonthYearToDate)
            df = df.set_index('Date')
        elif extra_handling == 'Users':
            # Count the number of new users by month
            df = df.groupby('Date').count()
            # Create cumulative sum
            df['Total'] = df.NewUsers.cumsum()
        df.to_pickle(DATA_DIR + filename)
    else:
        # Load DataFrame from file
        print('Loading from file...')
        df = pd.read_pickle(DATA_DIR + filename)
        # Truncate if needed
        if truncate:
            df = df.reset_index()
            df = truncate_date_df(df, col_name='Date',
                                  from_date='2003-01-01', to_date='2014-07-01')
            df = df.set_index('Date')
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


def find_users_first_review(acc, file_path):
    '''
    Find the users first reviews in a given file and update the values in an accumulator
    '''
    skiped = 0
    g = gzip.open(file_path, 'rb')
    for l in g:
        row = eval(l)
        try:
            # Get ReviewerID
            user = get_user(row)
            if user is None:
                # If no ReviewerID go to next
                continue
            # Get review's date
            date_key = getDate(row).strftime('%Y-%m')
            # Update accumulator
            if user in acc and acc[user] > date_key:
                acc[user] = date_key
            elif user not in acc:
                acc[user] = date_key

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


def get_new_products(acc, file_path):
    '''
    find the lauch date of all products in a given file and update the values in an accumulator
    '''
    skiped = 0
    g = gzip.open(file_path, 'rb')
    for l in g:
        row = eval(l)
        try:
            # Get ReviewerID
            product = get_product(row)
            if product is None:
                # If no ReviewerID go to next
                continue
            # Update accumulator
            if product not in acc:
                acc[product] = datetime.datetime.today()
        except (KeyError, ValueError) as e:
            #print('row is : {}'.format(row))
            skiped += 1
            continue
    if skiped:
        print('skipped {} rows because of KeyError (not present) or ValueError (not parsable)'.format(skiped))


def update_procut_acc(acc, file_path):
    '''
    update the lauch date in acc according to a given reviews file
    '''
    skiped = 0
    g = gzip.open(file_path, 'rb')
    for l in g:
        row = eval(l)
        try:
            # Get ReviewerID
            product = get_product(row)
            if product is None:
                # If no ReviewerID go to next
                continue
            # Get review's date
            date_key = getDate(row)
            # Update accumulator
            if product in acc and acc[product] > date_key:
                acc[product] = date_key
        except (KeyError, ValueError) as e:
            #print('row is : {}'.format(row))
            skiped += 1
            continue
    if skiped:
        print('skipped {} rows because of KeyError (not present) or ValueError (not parsable)'.format(skiped))


def load_products_lauch(filename, truncate=False):
    '''
    Load a DataFrame of the products lauch date
    '''
    # Check if the file was already computed
    if not os.path.isfile(DATA_DIR + filename):
        print('Computing file...')
        acc = {}

        # Find all product
        logs = tqdm(os.listdir(DATA_DIR))
        for file in logs:
            logs.set_description(file)
            # Only take reviews files
            if (file.startswith('meta') and file.endswith('.json.gz')):
                get_new_products(acc, DATA_DIR + file)

        # Update the lauch date
        for file in logs:
            logs.set_description(file)
            # Only take reviews files
            if (file.startswith('reviews') and file.endswith('.json.gz')):
                update_procut_acc(acc, DATA_DIR + file)

        # Convert to DataFrame
        df = pd.DataFrame(list(acc.items()), columns=['asin', 'Date']).sort_values('Date')
        # Truncate to take only relevant time frame
        if truncate:
            df = truncate_date_df(df, col_name='Date',
                                  from_date='2003-01-01', to_date='2014-07-01')

        df.to_pickle(DATA_DIR + filename)
    else:
        # Load DataFrame from file
        print('Loading from file...')
        df = pd.read_pickle(DATA_DIR + filename)
        # Truncate if needed
        if truncate:
            df = df.reset_index()
            df = truncate_date_df(df, col_name='Date',
                                  from_date='2003-01-01', to_date='2014-07-01')
    return df


def add_lauch_date(lauch_dic, products_df):
    ''' Add a column with the lauch date of the products to a DataFrame '''
    values = lauch_dic[lauch_dic.asin.isin(products_df.asin)].set_index('asin')
    products_df = products_df.set_index('asin')
    products_df['Lauched'] =  values
    return products_df.reset_index()