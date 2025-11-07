from typing import List

from elasticsearch import Elasticsearch
from tqdm import tqdm
import json
from sentence_transformers import SentenceTransformer
from final_project.getting_started.backend.utils import get_es_client
from final_project.getting_started.backend.config import INDEX_NAME_EMBEDDING


# Creating a connection to Elasticsearch
def index_data(documents: List[dict], model: SentenceTransformer): 
    es = get_es_client(maxretries=3, sleep_time=5)
    _ = _create_index(es=es)
    _ = _insert_documents(es=es, documents=documents, model=model)


# Creating the index
def _create_index(es: Elasticsearch) -> dict:
    index_name = INDEX_NAME_EMBEDDING
    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(
        index=index_name,
        mappings={
            "properties": {
                "embedding": {
                    "type": "dense_vector",
                }
            }
        }
        
    )

# Inserting documents into the index with bulk API
def _insert_documents(es: Elasticsearch, documents: List[dict], model: SentenceTransformer) -> dict:
    operations = []
    index_name = INDEX_NAME_EMBEDDING
    for document in tqdm(documents, total=len(documents), desc="Indexing documents"):
        operations.append({
            'index': {
                '_index': index_name,
            }
        })
        operations.append({
            **document,
            'embedding': model.encode(document['explanation'])
        })
    return es.bulk(operations=operations)


if __name__ == "__main__":
    with open('/Users/visakh/elastic/ElasticSearch_Python_Course/data/apod.json', 'r') as f:
        documents = json.load(f)
    # device = 'cuda' if torch.cuda.is_available() else 'cpu'
    model = SentenceTransformer('all-MiniLM-L6-v2')
    index_data(documents=documents, model=model)