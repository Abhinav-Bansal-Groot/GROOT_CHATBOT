from app.core.config import settings

META_QUESTIONS = [
    "what is your name",
    "who are you",
    "whom am i talking with",
    "who created you",
    "tell me about yourself"
]

def check_meta_question(query: str) -> str | None:
    """Return deterministic response if query is about chatbot identity."""
    q = query.lower()
    if any(mq in q for mq in META_QUESTIONS):
        return f"My name is {settings.PROJECT_NAME}, your {settings.BOT_ROLE} 🤖"
    return None
