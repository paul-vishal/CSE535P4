import os
import pandas as pd
from indexer import Indexer
from app import tweetnews


def main():
    indexer = Indexer()
    pos = 0
    neg = 0
    mixed = 0
    neutral = 0
    for filename in os.listdir('data_latest'):
        print(filename)
        with open('data_latest/' + filename, 'rb') as f:
            try:
                data = pd.read_pickle(f)
                json_ = data.to_dict(orient='records')
                indexer.create_documents(json_)
                for json_s in json_:
                    if json_s['sentiment'] == 'POSITIVE':
                        pos = pos + 1
                    if json_s['sentiment'] == 'NEGATIVE':
                        neg = neg + 1
                    if json_s['sentiment'] == 'MIXED':
                        mixed = mixed + 1
                    if json_s['sentiment'] == 'NEUTRAL':
                        neutral = neutral + 1

            except Exception as e:
                print(e)
    total = pos + neg + mixed + neutral
    print('Positive:', pos)
    print('Negative:', neg)
    print('Mixed:', mixed)
    print('Neutral:', neutral)
    print('Total:', total)


def test_tweet_news(query):
    docs = tweetnews(query)
    print(docs)


if __name__ == "__main__":
    #test_tweet_news("vaccine is effective")
    main()
