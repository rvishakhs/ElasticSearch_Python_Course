from typing import List

from elasticsearch import Elasticsearch
from tqdm import tqdm
import json

from final_project.getting_started.backend.utils import get_es_client
from final_project.getting_started.backend.config import INDEX_NAME, INDEX_NAME_N_GRAM


# Creating a connection to Elasticsearch
def index_data(documents: List[dict], use_n_gram_tokenizer: bool = False): 
    es = get_es_client(maxretries=3, sleep_time=5)
    _ = _create_index(es=es, use_n_gram_tokenizer= use_n_gram_tokenizer)
    _ = _insert_documents(es=es, documents=documents, use_n_gram_tokenizer= use_n_gram_tokenizer)


# Creating the index
def _create_index(es: Elasticsearch, use_n_gram_tokenizer: bool) -> dict:
    tokenizer = 'n_gram_tokenizer' if use_n_gram_tokenizer else 'standard'
    index_name = INDEX_NAME_N_GRAM if use_n_gram_tokenizer else INDEX_NAME
    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(
        index=index_name,
        body={
            "settings": {
                "analysis": {
                    "analyzer": {
                        "default": {
                            "type": "custom",
                            "tokenizer": tokenizer,
                        }
                    },
                    "tokenizer": {
                        "n_gram_tokenizer": {
                            "type": "edge_ngram",
                            "min_gram": 1,
                            "max_gram": 20,
                            "token_chars": [
                                "letter",
                                "digit"
                            ]
                        }
                    }
                }
            }
        }
        
    )

# Inserting documents into the index with bulk API
def _insert_documents(es: Elasticsearch, documents: List[dict], use_n_gram_tokenizer: bool) -> dict:
    operations = []
    index_name = INDEX_NAME_N_GRAM if use_n_gram_tokenizer else INDEX_NAME
    for document in tqdm(documents, total=len(documents), desc="Indexing documents"):
        operations.append({
            'index': {
                '_index': index_name,
            }
        })
        operations.append(document)
    return es.bulk(operations=operations)


if __name__ == "__main__":
    with open('/Users/visakh/elastic/ElasticSearch_Python_Course/data/apod.json', 'r') as f:
        documents = json.load(f)
    index_data(documents=documents, use_n_gram_tokenizer=True)