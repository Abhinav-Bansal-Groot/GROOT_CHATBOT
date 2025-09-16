from langchain_core.documents import Document
from app.core import config


class RetrievalService:
    def __init__(self, qdrant_repo, embedder):
        self.qdrant = qdrant_repo
        self.embedder = embedder


    def retrieve(self, query: str, top_k: int = 3) -> list[Document]:
        qvec = self.embedder.embed(query)
        docs = []
        for coll in [c.lower() + "_collection" for c in config.settings.COLLECTIONS]:
            results = self.qdrant.search(coll, qvec, limit=top_k)
            for r in results:
                payload = r.payload or {}
                text = " ".join([str(v) for v in payload.values() if isinstance(v, str)])
                docs.append(Document(page_content=text, metadata={"source": coll}))
        return docs