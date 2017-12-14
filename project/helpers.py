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
        logs= tqdm(os.listdir(DATA_DIR))
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
        elif extra_handling in ['Users', 'Products'] :
            new_df = pd.DataFrame(list(acc_new.items()), columns= ['New', 'datetime'])
            # Map list to count
            acc_active = {k: len(v) for k, v in acc_active.items()}
            active_df = pd.DataFrame(list(acc_active.items()), columns=['datetime', 'Active'])

            new_df      = new_df.groupby([new_df.datetime.dt.year, new_df.datetime.dt.month]).count()
            active_df   = active_df.groupby([active_df.datetime.dt.year, active_df.datetime.dt.month]).sum()
            df = new_df.join(active_df, how='outer').drop('datetime', axis=1)

        df.index.names = ['Year','Month']
        df['Total'] = df.New.cumsum()
        df = df.fillna(0)
        df.to_pickle(DATA_DIR + filename)
    else:
        # Load DataFrame from file
        print('Loading from file...')
        df = pd.read_pickle(DATA_DIR + filename)
    # Truncate to take only relevant time frame
    if truncate:
        df = df.loc[(df.index.get_level_values('Year') > 2003) & (df.index.get_level_values('Year') < 2014)]

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

PRODUCTS_FILE = 'products_lauch_df'
def load_products_lauch():
    '''
    Load a DataFrame of the products lauch date
    '''
    # Check if the file was already computed
    if not os.path.isfile(DATA_DIR + PRODUCTS_FILE):
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
        df = truncate_date_df(df, col_name='Date',
                                from_date='2003-01-01', to_date='2014-07-01')

        df.to_pickle(DATA_DIR + PRODUCTS_FILE)
    else:
        # Load DataFrame from file
        print('Loading {} from file...'.format(PRODUCTS_FILE))
        df = pd.read_pickle(DATA_DIR + PRODUCTS_FILE)
        # Truncate if needed
        df = df.reset_index()
        df = truncate_date_df(df, col_name='Date',
                                from_date='2003-01-01', to_date='2014-07-01').drop('index', axis=1)
        df.columns = ['new_products', 'Date']
    return df


def add_lauch_date(lauch_dic, products_df):
    ''' Add a column with the lauch date of the products to a DataFrame '''
    values = lauch_dic[lauch_dic.new_products.isin(products_df.asin)].set_index('new_products')['Date']
    products_df = products_df.set_index('asin')
    products_df['datetime'] =  values
    return products_df.reset_index()

FOOD_LAUCH = 'food_lauch_df'
SPORT_LAUCH = 'sport_lauch_df'

def load_df_lauch(product_df, filename):
    if not os.path.isfile(DATA_DIR + filename):
        # Get the lauch date of all products
        products_lauch = load_products_lauch()
        df = add_lauch_date(products_lauch, product_df)
        df.to_pickle(DATA_DIR + filename)
    else:
        print('Loading {} from file...'.format(filename))
        df = pd.read_pickle(DATA_DIR + filename)
    return df

def load_sport_lauch(sport_df):
    return load_df_lauch(sport_df, SPORT_LAUCH)

def load_food_lauch(food_df):
    return load_df_lauch(food_df, FOOD_LAUCH)


PRODUCTS_COUNT = 'products_count_df'

def load_products_count():
    if not os.path.isfile(DATA_DIR + PRODUCTS_COUNT):
        products_lauch = load_products_lauch()
        products_lauch.Date = products_lauch.Date.apply(convert_Date_To_Month)
        df = products_lauch.groupby('Date').count()
        df['Total'] = df.new_products.cumsum()
        df.to_pickle(DATA_DIR + PRODUCTS_COUNT)
    else:
        print('Loading {} from file...'.format(PRODUCTS_COUNT))
        df = pd.read_pickle(DATA_DIR + PRODUCTS_COUNT)
        df = df.reset_index()
        df = df.groupby([df.Date.dt.year, df.Date.dt.month]).sum()
    return df


def get_trend(reviews_df, column, reviewers_df, products_df, from_year = 2003):
    trend_df = reviews_df[[column]].copy()
    trend_df[column] = trend_df[column] / products_df.Total / reviewers_df.Total

    # Resetting index
    trend_df = trend_df.reset_index(0)
    trend_df.columns = ['year', column]
    trend_df = trend_df.reset_index()
    trend_df.columns = ['month', 'year', column]

    # Truncate
    trend_df = trend_df[trend_df.year > from_year].set_index(['year', 'month'])
    return trend_df