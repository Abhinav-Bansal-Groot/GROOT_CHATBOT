from fastapi import FastAPI
from app.api.routes import router as api_router


app = FastAPI(title="RAG FastAPI (Clean Architecture) some changes")
app.include_router(api_router)


