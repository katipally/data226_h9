# Import necessary libraries
import pandas as pd
from vespa.application import Vespa
from vespa.io import VespaQueryResponse

def display_hits_as_df(response: VespaQueryResponse, fields) -> pd.DataFrame:
    records = []
    for hit in response.hits:
        record = {}
        for field in fields:
            record[field] = hit["fields"].get(field, None)
        records.append(record)
    return pd.DataFrame(records)

def keyword_search(app, search_query):
    query = {
        "yql": "select * from sources * where userQuery() limit 5",
        "query": search_query,
        "ranking": "bm25",
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title", "text"])

def semantic_search(app, search_query):
    query = {
        "yql": "select * from sources * where ({targetHits:100}nearestNeighbor(embedding,e)) limit 5",
        "query": search_query,
        "ranking": "semantic",
        "input.query(e)": "embed(@query)"
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title", "text"])

def get_embedding(app, doc_id):
    query = {
        "yql": f"select doc_id, title, text, embedding from content.doc where doc_id contains '{doc_id}'",
        "hits": 1
    }
    result = app.query(query)
    
    if result.hits:
        return result.hits[0]["fields"]["embedding"]
    return None

def query_movies_by_embedding(app, embedding_vector):
    query = {
        'hits': 5,
        'yql': 'select * from content.doc where ({targetHits:5}nearestNeighbor(embedding, user_embedding))',
        'ranking.features.query(user_embedding)': str(embedding_vector),
        'ranking.profile': 'recommendation'
    }
    return app.query(query)

app = Vespa(url="http://localhost", port=8082)

query = "The Godfather"  

df_keyword = keyword_search(app, query)
print("Keyword Search Results:")
print(df_keyword.head())

df_semantic = semantic_search(app, query)
print("\nSemantic Search Results:")
print(df_semantic.head())

doc_id = "1"  
embedding = get_embedding(app, doc_id)
if embedding:
    recommendation_results = query_movies_by_embedding(app, embedding)
    df_recommendation = display_hits_as_df(recommendation_results, ["doc_id", "title", "text"])
    print("\nRecommendation Search Results:")
    print(df_recommendation.head())
else:
    print("Embedding not found for the given doc_id.")
