import os
import pandas as pd
import boto3


def get_tweets():
    directory = 'D:\CSE535 - Information Retreival\Project1\CSE_4535_Fall_2021\project1\data_backuplatest\\'
    for filename in os.listdir(directory):
        print(filename)
        with open(directory + filename, 'rb') as f:
            pickle_file_content = pd.read_pickle(f)
            data_json = pickle_file_content.to_dict(orient='records')
            updated_json_list = []
            for data in data_json:
                text = data['tweet_text']
                sentiment, sentiment_score = get_sentiment_score(text)
                data['sentiment_score'] = sentiment_score
                data['sentiment'] = sentiment
                updated_json_list.append(data)
            save_file(updated_json_list, filename.split('.')[0] + "_updated.pkl")


def save_file(data, filename):
    df = pd.DataFrame(data)
    df.to_pickle("data/" + filename)


def get_sentiment_score(text):
    comprehend = boto3.client(service_name='comprehend', region_name='us-east-2',
                              aws_access_key_id='AKIAVQIGLH3RB3I3YCEL',
                              aws_secret_access_key='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')
    sentiment_json = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    sentiment = sentiment_json['Sentiment']
    score = sentiment_json['SentimentScore'][sentiment.capitalize()]
    return sentiment, score


if __name__ == "__main__":
    # s, sc = get_sentiment_score("hi hello world")
    # print(s)
    # print(sc)
    get_tweets()
