# app/services/chat_service.py
from app.ai.meta_handler import check_meta_question
from app.core.config import settings
from langchain_core.documents import Document   # <- fix: ensure Document is defined for annotations

class ChatService:
    def __init__(self, retriever, llm):
        """
        retriever: should be an instance of your RetrievalService (has .retrieve(query) -> list[Document])
        llm: should be your LLM wrapper (has .generate(prompt) -> str)
        """
        self.retriever = retriever
        self.llm = llm

    def ask(self, query: str) -> dict:
        """
        Backwards-compatible RAG method that returns {answer: str, sources: list}
        (keeps the previous behavior you had).
        """
        docs = self.retriever.retrieve(query)
        if not docs:
            return {"answer": "⚠️ No relevant documents found.", "sources": []}

        context = "\n\n".join([f"Source: {d.metadata.get('source','')}\n{d.page_content}" for d in docs])
        prompt = (
            f"You are a helpful assistant. Use the following context to answer the question.\n\n"
            f"Context:\n{context}\n\nQuestion: {query}\n\nAnswer:"
        )
        ans = self.llm.generate(prompt)  # expects a string
        sources = [d.metadata for d in docs[:3]]
        return {"answer": ans, "sources": sources}

    def chat_logic(self, query: str) -> str:
        """
        Preferred, thin chat entrypoint:
        - handles meta-questions deterministically (uses meta_handler)
        - else runs RAG (retrieval -> LLM) and returns a plain string answer
        """
        # 1) Deterministic meta-question handling
        meta_response = check_meta_question(query)
        if meta_response:
            return meta_response

        # 2) Retrieval
        docs = self.retriever.retrieve(query)
        if not docs:
            return "⚠️ No relevant documents found."

        # 3) Generate answer using internal helper
        return self._generate_answer(query, docs)

    def _generate_answer(self, query: str, retrieved_docs: list[Document]) -> str:
        """
        Internal generator that builds the system prompt + context and asks LLM.
        Returns a plain string.
        """
        context_text = "\n\n".join([doc.page_content for doc in retrieved_docs])
        system_prompt = (
            f"You are {settings.PROJECT_NAME}, a helpful {settings.BOT_ROLE}.\n"
            f"If asked about yourself, always answer truthfully with this identity."
        )

        prompt = (
            f"{system_prompt}\n\n"
            f"Context:\n{context_text}\n\n"
            f"Question: {query}\n\n"
            f"Answer:"
        )

        # Use the LLM wrapper's generate method (returns a string)
        result = self.llm.generate(prompt)

        # If the wrapper returns an object, try to normalize
        if hasattr(result, "content"):
            return result.content
        return str(result)
