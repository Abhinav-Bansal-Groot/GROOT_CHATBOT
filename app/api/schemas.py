from pydantic import BaseModel


class ChatRequest(BaseModel):
    question: str


class ChatResponse(BaseModel):
    answer: str


class SQLToMongoRequest(BaseModel):
    sql_conn_str: str
    mongo_conn_str: str
    mongo_db_name: str
    table_name: str