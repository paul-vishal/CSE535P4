import requests
from flask import Flask, render_template, jsonify, request
import urllib.request
import urllib

app = Flask(__name__)
AWS_IP = '18.221.65.93'
AWS_PORT = '8983'
SOLR_CORE_NAME = 'IR_P4'


@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/tweet_search')
def tweetsearch():
    print('tweetsearch')
    input_query = request.args.get('query')
    if input_query is None or input_query == '':
        return render_template('basic_page.html')
    encoded_query = preprocess(input_query)
    print(encoded_query, 'this is encoded query')
    query_url = 'http://' + AWS_IP + ':' + AWS_PORT + '/solr/' + SOLR_CORE_NAME + '/select?fl=id%20score%20tweet_text%20sentiment%20sentiment_score&q=text_en%3A(' + encoded_query + ')%20or%20text_hi%3A(' + encoded_query + ')%20or%20text_es%3A(' + encoded_query + ')&rows=20'
    data = requests.get(query_url).json()
    docs = data['response']['docs']
    news_docs = tweetnews(input_query)
    return render_template('basic_page.html', tweet_search=docs, tweet_news=news_docs)


def tweetnews(query):
    print('tweetnews')
    input_query = query
    encoded_query = preprocess(input_query)
    print(encoded_query, 'this is encoded query')
    news_query_url = 'https://newsapi.org/v2/everything?q=' + encoded_query + '&from=2021-12-06&apiKey=ae9778d05cd74f219e4fcaf7afad1c3a'
    news_data = requests.get(news_query_url).json()
    print('got data', news_data)
    news_docs = news_data['articles']
    news_docs_20 = []
    for article in news_docs:
        news_docs_20.append(article)
    return news_docs_20


def preprocess(query):
    query = query.strip('\n').replace(':', '')
    query = urllib.parse.quote(query)
    return query


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9999)
