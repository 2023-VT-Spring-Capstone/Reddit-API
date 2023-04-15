from elasticsearch import Elasticsearch

es = Elasticsearch(['http://localhost:9200'], http_auth=('elastic', 'lIaOuoKHcJcM173Ei8U7'))

search_ticker = 'BB'
upvote_ratio = 0.98
response = es.search(index='stock_sentiment_analysis', query={"match": {"upvote_ratio": upvote_ratio}})

print("Documents in stock_sentiment_analysis index:")
for hit in response['hits']['hits']:
    print(hit["_source"])


