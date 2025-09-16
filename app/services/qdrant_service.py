from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
from app.core import config


class QdrantService:
    def __init__(self, url: str = config.settings.QDRANT_URL):
        self.client = QdrantClient(url=url)


    def recreate_collection(self, name: str, vector_size: int):
        self.client.recreate_collection(
            collection_name=name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )


    def upsert_points(self, collection_name: str, points: list[PointStruct]):
        # Qdrant SDK handles batching on server, but we upsert in caller batches
        self.client.upsert(collection_name=collection_name, points=points, wait=True)


    def search(self, collection_name: str, query_vector: list[float], limit: int = 3):
        return self.client.search(collection_name=collection_name, query_vector=query_vector, limit=limit)