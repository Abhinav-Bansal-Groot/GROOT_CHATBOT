# app/repositories/chat_repository.py
from app.services.chat_service import ChatService

class ChatRepository:
    def __init__(self, chat_service: ChatService):
        self.chat_service = chat_service

    def handle_chat(self, question: str) -> dict:
        """
        Wrapper around ChatService methods.
        - First tries meta-question logic (chat_logic).
        - Falls back to RAG ask() when needed.
        - Always returns a dict { "answer": str, "sources": list }.
        """

        # Use chat_logic (handles meta-questions and lightweight RAG)
        meta_or_simple_answer = self.chat_service.chat_logic(question)
        if isinstance(meta_or_simple_answer, str):
            return {"answer": meta_or_simple_answer, "sources": []}

        # Otherwise, use full RAG pipeline
        return self.chat_service.ask(question)
