from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File, Form

from app.infrastructure.mongo_repository import MongoRepository
from app.infrastructure.chat_repository import ChatRepository
from app.infrastructure.sql_to_mongo_repository import SQLToMongoRepository

from app.services.sql_to_mongo_service import SQLToMongoService
from app.services.ingest_service import IngestionService
from app.services.retrieval_service import RetrievalService
from app.services.chat_service import ChatService

from app.services.qdrant_service import QdrantService
from app.ai.llm import LLM
from app.ai.embeddings import Embeddings

from .schemas import ChatRequest, ChatResponse, SQLToMongoRequest
import json
import os

router = APIRouter(prefix="/api")


def get_services():
    mongo = MongoRepository()
    qdrant = QdrantService()
    embedder = Embeddings()
    llm = LLM()
    ingest = IngestionService(mongo, qdrant, embedder)
    retr = RetrievalService(qdrant, embedder)
    chat_service = ChatService(retr, llm)
    chat_repo = ChatRepository(chat_service)   
    return {"mongo": mongo, "qdrant": qdrant, "ingest": ingest, "embedder": embedder, "retr": retr, "chat": chat, "chat_repo": chat_repo}


@router.post("/sql-to-mongo")
def sql_to_mongo(body: SQLToMongoRequest):
    table_name = "".join(word.capitalize() for word in body.table_name.split())

    repo = SQLToMongoRepository(body.sql_conn_str, body.mongo_conn_str, body.mongo_db_name)
    service = SQLToMongoService(repo)

    result = service.migrate_table(table_name)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/seed")
def seed(request: Request):
    # Reads data/data.json from disk and seeds MongoDB
    path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "data.json")
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise HTTPException(status_code=400, detail=f"Seed file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        seed_data = json.load(f)
    services = get_services()
    services["mongo"].seed_collections(seed_data)
    return {"seeded_collections": list(seed_data.keys())}


@router.post("/ingest")
def ingest():
    services = get_services()
    result = services["ingest"].ingest_all()
    return {"result": result}


@router.post("/chat", response_model=ChatResponse)
def chat(body: ChatRequest):
    services = get_services()
    res = services["chat_repo"].handle_chat(body.question)
    return {"answer": res["answer"]}
