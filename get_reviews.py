import requests
import urllib.parse
import json
import multiprocessing as mp
import review_config



total_reviews = []

def get_reviews_for_app(app_id):

    """
    Get reviews for an game/app with app_id
    :param app_id: game/app id
    :return: list of reviews for app_id
    """

    print(f"Retrieve reviews for app id {app_id}")
    url = review_config.BASE_URL + app_id +'?'
    curr_cursor = None
    reviews = []
    params = {
        'json': 1,
        'filter': 'recent',
    }
    total_count = 0


    while True:
        if curr_cursor:
            params['cursor'] = curr_cursor
        try:
            response = requests.get(url + urllib.parse.urlencode(params))
            data = response.json()
            if data['success'] != 1 or data['query_summary']['num_reviews'] == 0 or total_count >= review_config.MAX_REVIEWS:
                break
            curr_cursor = data['cursor']
            total_count += int(data['query_summary']['num_reviews'])


            for review in data['reviews']:
                reviews.append(dict(
                    # steam id of user
                    steamid = review['author']['steamid'],
                    # app or game id which is reviewed
                    appid = app_id,
                    # total playtime of user
                    total_playtime = review['author'].get('playtime_forever', 0),
                    # play time at the moment user put this review
                    playtime_at_review = review['author'].get('playtime_at_review', 0),
                    # last play time (given in unix epoch time format)
                    last_play_time = review['author']['last_played'],
                    # recommended is true if user like this game, false otherwise
                    recommended = review['voted_up'],
                    helpful_vote = review['votes_up'], 
                    funny_vote = review['votes_funny'],
                    weighted_vote_score = review['weighted_vote_score'],
                    # user review text
                    content = review['review'],
                    # review created time
                    created_time = review['timestamp_created'], 
                    # last review updated time
                    last_updated = review['timestamp_updated']
                ))
        except Exception as e:
            print(e)
    return reviews


def add_result(result):
    """
    Concatenate the result.
    :param result: list of reviews for an app.
    """
    global total_reviews
    total_reviews += result

def process_multithreading(links: list):
    """
    Process get reviews with multi processes.
    :param links: list of app id.
    """
    pool = mp.Pool(processes=8)
    for app in links:
        pool.apply_async(get_reviews_for_app, args = (app["app id"], ), callback = add_result)
    pool.close()
    pool.join()

if __name__ == '__main__':
    link_file = open(review_config.INPUT_FILE)
    links = json.load(link_file)
    process_multithreading(links)
    file = open(review_config.OUTPUT_FILE, 'w')
    json.dump(total_reviews, file, indent=4)
