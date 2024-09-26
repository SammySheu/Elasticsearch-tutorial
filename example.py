from elasticsearch import Elasticsearch, helpers
from datetime import datetime

# Connect to Elasticsearch
es = Elasticsearch(["http://192.168.1.168:9200"])  # Replace with your Elasticsearch host

def insert_data(index_name, data):
    """
    Insert data into Elasticsearch.
    :param index_name: Name of the index
    :param data: List of dictionaries containing the data to insert
    """
    actions = [
        {
            "_index": index_name,
            "_source": item
        }
        for item in data
    ]
    
    successes, failures = helpers.bulk(es, actions, raise_on_error=False)
    print(f"Successfully inserted {successes} documents")
    if failures:
        print(f"Failed to insert {len(failures)} documents")

def query_data(index_name, query, size=10, sort_field="FromTime", sort_order="desc"):
    """
    Query data from Elasticsearch.
    :param index_name: Name of the index
    :param query: Query in Elasticsearch DSL format
    :param size: Number of results to return
    :param sort_field: Field to sort by
    :param sort_order: Sort order ('asc' or 'desc')
    """
    body = {
        "size": size,
        "query": query,
        "sort": [{sort_field: {"order": sort_order}}]
    }
    
    results = es.search(index=index_name, body=body)
    return results['hits']['hits']

# Example usage
if __name__ == "__main__":
    index_name = "alr_test"
    
    # Insert data
    # sample_data = [
    #     {"FromTime": datetime.now().isoformat(), "EventType": "Warning", "Description": "High CPU Usage"},
    #     {"FromTime": datetime.now().isoformat(), "EventType": "Error", "Description": "Database Connection Lost"}
    # ]
    # insert_data(index_name, sample_data)
    
    # Query data
    query = {
        "bool": {
            "must_not": [
                {"term": {"EventType": "Info"}}
            ]
        }
    }
    results = query_data(index_name, query)
    
    # Print results
    for hit in results:
        print(hit['_source'])