import sys, os, getopt, time, math

import requests
import json
import pandas as pd

#majority of this code is taken from:
#https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(keyword, start_date, end_date, max_results = 10):

    search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'tweet.fields': 'id,text,author_id,created_at',
                    'next_token': {}}
    return (search_url, query_params)



def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    #print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def get_tweets(url, headers, max_tweets):
    json_response = connect_to_endpoint(url[0], headers, url[1])
    df = pd.DataFrame(json_response['data'])


    next_token = json_response['meta']['next_token']
    count = json_response['meta']['result_count']

    while(count < max_tweets):
        time.sleep(5)
        json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
        df = df.append(pd.DataFrame(json_response['data']))
        next_token = json_response['meta']['next_token']
        count = count + json_response['meta']['result_count']
        if(next_token == None):
            break

    return([df, count])



def main(argv):
    
    try:
        opts, args = getopt.getopt(argv,"ho:")
    except getopt.GetoptError:
        print('collect_tweets.py -o <output_file>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print('collect_tweets.py -o <output_file>')
            sys.exit()
        elif opt in ("-o"):
            outputfile = arg

    bearer_token = 'AAAAAAAAAAAAAAAAAAAAAKUoVwEAAAAAhZS6MvLBWaOHUH%2F8RRRZ4UziP9s%3D4zc3CO5ta4K96NmjLktM38ZYiwrdIMfee5LooVxJvqTcohoRaq'
    headers = create_headers(bearer_token)
    keyword = '("king richard" OR #kingrichard OR @KingRichardFilm) lang:en -is:reply -is:retweet'
    start_time = "2021-11-19T00:00:00.000Z"
    end_times = ["2021-11-21T17:00:00.000Z", "2021-11-20T21:00:00.000Z", "2021-11-20T17:00:00.000Z", "2021-11-19T21:00:00.000Z", "2021-11-19T17:00:00.000Z"]
    max_results = 100
    total_tweets = 1000


    urls = [create_url(keyword, start_time, time, max_results) for time in end_times]
    count = 0

    tweets = pd.DataFrame()

    for url in urls:
        temp = get_tweets(url, headers, math.ceil(total_tweets/len(urls)))
        count = count + temp[1]
        tweets = tweets.append(temp[0])
        print('Collected ' + str(count) + ' tweets')

    tweets = tweets.drop_duplicates(subset=['id'])
    print('Total tweets collected: ' + str(count))
    print('Total tweets in dataframe: ' + str(tweets.shape[0]))
    try:
        tweets.to_csv(outputfile, index=False)
    except OSError:
        print('Error trying to write to: ' + outputfile)
        sys.exit()


if __name__ == '__main__':
    main(sys.argv[1:])
