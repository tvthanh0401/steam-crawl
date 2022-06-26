from decimal import Decimal
import boto3
import pandas as pd
from datetime import datetime, date
import re
import json
from multiprocessing import Process


def get_data_from_s3(bucket_name, file_name):
    """get all data from bucketS3 to json type

    Args:
        bucket_name (string): name of the bucket
        file_name (string): name of the file

    Returns:
        json: rawl table s3
    """
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket_name, file_name)
    json_content = json.loads(
        content_object.get()['Body'].read().decode('utf-8'))
    return json_content


def get_secret_from_secretmanager(secret_name, region_name):
    """get access key id and secret access key fromsecretmanager

    Args:
        secret_name (string): secret name
        region_name (string): region name

    Returns:
        dictionary: secret access key id and secret access key of account AWS
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    response = client.get_secret_value(SecretId=secret_name)

    # convert string(response) to dictionary(secrets)
    secrets = json.loads(response['SecretString'])
    return secrets


def create_dynamodb(secrets):
    """create dynamodb in bucketS3

    Args:
        secrets (dictionary): secret access key id and secret access key of account AWS

    Returns:
        dynamo_session: service dynamodb aws
    """
    name = 'dynamodb'  # name dynamodb in aws
    endpoint_url1 = 'https://dynamodb.us-west-2.amazonaws.com'  # link endpoint in region
    region_name1 = 'us-west-2'  # region in use
    # get access_key from secrets
    aws_access_key_id = secrets['aws_access_key_id']

    # get secret_key from secrets
    aws_secret_access_key = secrets['aws_secret_access_key']
    dynamodb = boto3.resource(name,
                              endpoint_url=endpoint_url1,
                              region_name=region_name1,
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)
    return dynamodb


def link_to_id(link):
    """find id from link in dataframe link

    Args:
        link (string): link of dataframe link

    Returns:
        int: app id of link
    """
    app_id = re.findall("(?<=\/)\d.+?(?=\/)", link)
    return int(app_id[0])


def date_to_timestamp(deal_day, deal_month):
    """convert data to timestamp and return year-month-day time

    Args:
        deal_day (int): day of data
        deal_month (int): month of data

    Returns:
        datetime: datetime of isoformat
    """
    # get year,month,day today
    todays_date = date.today()
    todays_year = todays_date.year
    todays_month = todays_date.month
    todays_day = todays_date.day
    # If current date is greater than today's date then increment current year by 1
    if todays_month > deal_month or (todays_month == deal_month and todays_day > deal_day):
        todays_year += 1
    dt = datetime(year=todays_year, day=deal_day, month=deal_month)
    return dt.isoformat()


def offerend_to_timestamp(offer_ends):
    """get the date in the offerend column

    Args:
        offer_ends (string): offer end of deal dataframe

    Returns:
        datetime: clean datetime
    """
    split_ends = offer_ends.split()  # get list text split " " in offerend column
    if (split_ends[-1] != "in"):
        day = int(datetime.strptime(split_ends[-1], '%d').strftime('%d'))
        month = int(datetime.strptime(split_ends[-2], '%B').strftime('%m'))
        return date_to_timestamp(day, month)
    return offer_ends


def date_to_dateTime(date):
    """convert date to Year-month-day and time is 0

    Args:
        date (date): date of data

    Returns:
        datetime: clean datetime
    """
    # handling exceptions
    try:
        # convert Month day year to year month day and time is 0
        dt = list(map(int, datetime.strptime(
            date, '%b %d, %Y').strftime("%Y-%m-%d").split("-")))
        return datetime(dt[0], dt[1], dt[2]).isoformat()
    except ValueError:
        try:
            # convert Month year to year month day with day is 01 and time is 0
            dt = list(map(datetime.strptime(
                date, '%b %Y').strftime("%Y-%m-%d").split("-")))
            return datetime(dt[0], dt[1], dt[2]).isoformat()
        except:
            return date
    except TypeError:
        # if data is in year month day then do notthing
        return date


def data_to_isoformat(date):
    """convert date to dateTime

    Args:
        date (date): date of data

    Returns:
        datetime: clea datetime
    """
    try:
        # convert date to dateTime
        date = int(round(date.timestamp()))
        return datetime.fromtimestamp(date).isoformat()
    except:
        # if data is in year month day then do notthing
        return date


def replace_name_columns(dataframe):
    """replace all name columns from " " to "_"

    Args:
        dataframe (dataframe): dataframe want to replace

    Returns:
        dataframe: replace all name columns
    """
    dataframe.columns = dataframe.columns.str.replace(
        ' ', '_')
    return dataframe


def pre_data_deal(df_deal):
    """preprocessing deal data

    Args:
        df_deal (dataframe): dataframe of deal data

    Returns:
        dataframe: data when pre processing
    """
    df_deal_process = df_deal.dropna(
        subset=["name"])  # drop name column Null

    df_deal_process["review"] = df_deal_process["review"].fillna(
        "No Review")  # replace Null to No review in review column

    df_deal_process["recent review"] = df_deal_process["recent review"].fillna(
        "No Review")  # replace Null to No review in recent review column

    df_deal_process["offer ends"] = df_deal_process["offer ends"].fillna(
        "Offer ends in")    # replace Null to No review in offer ends column

    df_deal_process['original price'] = df_deal_process['original price'].replace(
        r'^\$', '', regex=True)  # replace $ to space in original price column

    df_deal_process['discounted price'] = df_deal_process['discounted price'].replace(
        r'^\$', '', regex=True)  # replace $ to space in original discounted price column

    df_deal_process['timestamp'] = df_deal_process['timestamp'].fillna(
        df_deal_process['offer ends'].apply(offerend_to_timestamp))  # replace Null to DateTime in timestamp column

    df_deal_process['release date'] = df_deal_process['release date'].apply(
        date_to_dateTime)   # convert date to DateTime in release date column

    df_deal_process['timestamp'] = df_deal_process['timestamp'].apply(
        data_to_isoformat)  # convert timestamp to DateTime in timestamp column

    df_deal_process['app_id'] = df_deal_process['link'].apply(
        link_to_id)  # add app_id column from link column

    df_deal_process = replace_name_columns(df_deal_process)

    return df_deal_process


def pre_data_reviews(df_reviews):
    """preprocessing reviews data reviews data

    Args:
        df_reviews (dataframe): dataframe of reviews data

    Returns:
        dataframe: data when pre processing
    """
    df_reviews['last_play_time'] = df_reviews['last_play_time'].apply(
        data_to_isoformat)
    df_reviews['created_time'] = df_reviews['created_time'].apply(
        data_to_isoformat)
    df_reviews['last_updated'] = df_reviews['last_updated'].apply(
        data_to_isoformat)

    df_reviews = replace_name_columns(df_reviews)

    return df_reviews


def pre_data_link(df_link):
    """preprocessing link data

    Args:
        df_link (dataframe): dataframe of link data

    Returns:
        dataframe: data when preprocessing link data
    """
    df_link = replace_name_columns(df_link)
    return df_link


def load_data_deal(deals, secrets):
    """load data from deals_dataframe to steametl_deals_table in AWS dynamodb

    Args:
        deals (dataframe): dataframe of deals_dataframe
        secrets (dictionary): access key and secret key in aws
    """
    dynamodb = create_dynamodb(secrets)
    # call steametl_deals table in dynamodb
    deal_table = dynamodb.Table('steametl_deals')
    # use batch writer to load results into steametl_deals table
    with deal_table.batch_writer(overwrite_by_pkeys=["app_id", "end_date"]) as batch:
        for _, deal in deals.iterrows():
            app_id = link_to_id(deal['link'])
            # get columns want to load
            item = {'app_id': str(app_id),
                    'discounted_price': deal['discounted_price'],
                    'end_date': deal['timestamp']
                    }
            batch.put_item(Item=item)


def load_data_reviews(reviews, secrets):
    """load data from reviews_dataframe to steametl_reviews_table in AWS dynamodb

    Args:
        reviews (dataframe): dataframe of reviews_dataframe
        secrets (dictionary): access key and secret key in aws
    """
    dynamodb = create_dynamodb(secrets)
    # call steametl_reviews table in dynamodb
    review_table = dynamodb.Table('steametl_reviews')
    # use batch writer to load results into steametl_reviews table
    with review_table.batch_writer(overwrite_by_pkeys=["app_id", "steam_id"]) as batch:
        for _, review in reviews.iterrows():
            # get columns want to load
            item = {
                'app_id': str(review['appid']),
                'steam_id': str(review['steamid']),
                'total_playtime': review['total_playtime'],
                'playtime_at_review': review['playtime_at_review'],
                'last_play_time': review['last_play_time'],
                'recommended':  review['recommended'],
                'helpful_vote': review['helpful_vote'],
                'funny_vote': review['funny_vote'],
                'weighted_vote_score': Decimal(str(review['weighted_vote_score'])),
                'content': str(review['content']),
                'created_time': review['created_time'],
                'last_updated': review['last_updated']
            }
            batch.put_item(Item=item)


def load_data_game(games, secrets):
    """load data from secrets_dataframe to steametl_secrets_table in AWS dynamodb

    Args:
        games (dataframe): dataframe of game dataframe
        secrets (dictionary): access key and secret key in aws
    """
    dynamodb = create_dynamodb(secrets)
    # call steametl_games table in dynamodb
    game_table = dynamodb.Table('steametl_games')
    # use batch writer to load results into steametl_games table
    with game_table.batch_writer(overwrite_by_pkeys=["app_id", "name"]) as batch:
        for _, game in games.iterrows():
            # get columns want to load
            item = {'app_id': str(game['app_id_x']),
                    'name': str(game['name']),
                    'release_date': game['release_date'],
                    'tag': game['tag'],
                    'category': game['category'],
                    'developer':  game['developer'],
                    'review': game['review'],
                    'recent_review': game['recent_review'],
                    'original_price': game['original_price'],
                    'support_windows': game['support_windows'],
                    'support_mac':  game['support_mac'],
                    'support_linux': game['support_linux'],
                    'support_vr': game['support_vr']
                    }
            batch.put_item(Item=item)


def extract_data_from_s3(input_deal, input_link, input_reviews):
    """extract all data from bucketS3

    Args:
        input_deal (string): input path of deal data
        input_link (string): input path of link data
        input_reviews (string): input path of reviews data

    Returns:
        dataframe: dataframe all data from bucketS3
    """
    # read file json to dataframe
    df_deal = pd.DataFrame(input_deal)
    df_link = pd.DataFrame(input_link)
    df_reviews = pd.DataFrame(input_reviews)

    return df_deal, df_link, df_reviews


def transform_data_from_extract(df_reviews, df_link, df_deal):
    """transfrorm all data from extract_data_from_s3 function

    Args:
        df_reviews (dataframe): dataframe of reviews data
        df_link (dataframe): dataframe of link data
        df_deal (dataframe): dataframe of deal data

    Returns:
        dataframe: all dataframe of data
    """
    df_reviews = pre_data_reviews(df_reviews)  # processing reviews dataframe
    df_link = pre_data_link(df_link)  # processing link dataframe
    df_deal = pre_data_deal(df_deal)  # processing deal dataframe
    # make a dataframe game by join left deal dataframe and link dataframe
    df_game = df_deal.merge(df_link, on=['link', 'name'], how='left')

    return df_reviews, df_deal, df_game


def load_data_from_dataframe_to_dynamodb(df_reviews, df_deal, df_game, secrets):
    """load data from dataframe to dynamodb use multiprocessing thread

    Args:
        df_reviews (dataframe): dataframe of reviews data
        df_deal (dataframe): dataframe of deal data
        df_game (dataframe): dataframe of game data
        secrets (dictionary): access key and secret key in aws
    """
    reviews_process = Process(target=load_data_reviews,
                              args=(df_reviews, secrets))
    deal_process = Process(target=load_data_deal, args=(df_deal, secrets))
    game_process = Process(target=load_data_game, args=(df_game, secrets))

    reviews_process.start()
    deal_process.start()
    game_process.start()

    reviews_process.join()
    deal_process.join()
    game_process.join()


def main():
    now = datetime.now()
    current_day_str = f"{now.day}_{now.month}_{now.year}.json"
    input_deal = "deal/deal" + current_day_str
    input_link = "link/link" + current_day_str
    input_reviews = "reviews/reviews" + current_day_str

    bucket_name = "steametl"
    links = get_data_from_s3(bucket_name, input_link)
    deals = get_data_from_s3(bucket_name, input_deal)
    reviews = get_data_from_s3(bucket_name, input_reviews)

    df_deal, df_link, df_reviews = extract_data_from_s3(deals, links, reviews)
    df_reviews, df_deal, df_game = transform_data_from_extract(
        df_reviews, df_link, df_deal)

    secret_name = "arn:aws:secretsmanager:us-west-2:666243375423:secret:PhuocT2-DqBH8R"
    region_name = "us-west-2"
    secrets = get_secret_from_secretmanager(secret_name, region_name)

    load_data_from_dataframe_to_dynamodb(df_reviews, df_deal, df_game, secrets)


if __name__ == '__main__':
    main()
