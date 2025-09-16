from fastapi import FastAPI
from app.api.routes import router as api_router


app = FastAPI(title="RAG FastAPI (Clean Architecture)")
app.include_router(api_router)


