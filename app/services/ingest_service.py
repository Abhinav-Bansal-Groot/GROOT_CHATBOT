from qdrant_client.models import PointStruct
from app.core import config
from app.core.utils import serialize_doc_for_payload, doc_to_text


class IngestionService:
    def __init__(self, mongo_repo, qdrant_repo, embedder):
        self.mongo = mongo_repo
        self.qdrant = qdrant_repo
        self.embedder = embedder


    def ingest_collection(self, coll_name: str, batch_size: int | None = None):
        batch_size = batch_size or config.settings.BATCH_SIZE
        docs = self.mongo.get_documents(coll_name)
        if not docs:
            return {"collection": coll_name, "inserted": 0}


        qname = coll_name.lower() + "_collection"
        vector_size = config.settings.EMBEDDING_DIM
        # recreate collection with the expected vector size
        self.qdrant.recreate_collection(qname, vector_size)


        points = []
        for idx, doc in enumerate(docs):
            text = doc_to_text(doc, preferred_fields=["title", "description", "details", "bio", "name", "position", "service_name"])
            if not text:
                continue
            vec = self.embedder.embed(text)
            payload = serialize_doc_for_payload(doc)
            points.append(PointStruct(id=idx, vector=vec, payload=payload))


        # upsert in batches
        for i in range(0, len(points), batch_size):
            batch = points[i : i + batch_size]
            self.qdrant.upsert_points(qname, batch)


        return {"collection": coll_name, "inserted": len(points)}


    def ingest_all(self):
        results = []
        for coll in config.settings.COLLECTIONS:
            results.append(self.ingest_collection(coll))
        return results