from elasticsearch import Elasticsearch


INDEX_NAME = "finmesh-events"


def create_client() -> Elasticsearch:
    return Elasticsearch("http://localhost:9200")


def get_trade_lifecycle(trade_id: str) -> list[dict]:
    es = create_client()

    try:
        response = es.search(
            index=INDEX_NAME,
            size=100,
            query={
                "term": {
                    "trade_id.keyword": trade_id
                }
            },
            sort=[
                {
                    "timestamp": {
                        "order": "asc"
                    }
                }
            ],
        )

        return [
            hit["_source"]
            for hit in response["hits"]["hits"]
        ]

    finally:
        es.close()
