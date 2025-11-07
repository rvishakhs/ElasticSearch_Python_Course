import time

from elasticsearch import Elasticsearch

def get_es_client(maxretries: int = 1, sleep_time: int = 0) -> Elasticsearch:
    i = 0
    while i < maxretries:
        try:
            """Create and return an Elasticsearch client."""
            es = Elasticsearch(hosts=["http://localhost:9200"], basic_auth=("elastic", "iDfML4jw"))
            client_info = es.info()
            print(f"Connected to Elasticsearch cluster: {client_info['cluster_name']}")
            return es
        except Exception as e:
            print(f"Error connecting to Elasticsearch: {e}")
            time.sleep(sleep_time)
            i += 1
    raise ConnectionError("Failed to connect to Elasticsearch after multiple attempts.")
