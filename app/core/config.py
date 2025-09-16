from dotenv import load_dotenv
import os

load_dotenv()

class Settings:
    MONGODB_URI = os.getenv("MONGODB_URI")
    MONGO_DB = os.getenv("DB_NAME")
    QDRANT_URL = os.getenv("QDRANT_URL")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
    EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "3072"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "256"))
    COLLECTIONS = [c.strip() for c in os.getenv("COLLECTIONS", "Careers,ServicesOffereds,DirectorsInfo").split(",")]
    PROJECT_NAME: str = "Grootbot"  # chatbot name
    BOT_ROLE: str = "AI assistant"  # role/identity

    GPT_MODEL = os.getenv("GPT_MODEL")

settings = Settings()