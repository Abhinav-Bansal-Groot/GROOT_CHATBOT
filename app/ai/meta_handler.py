from app.core.config import settings


def check_meta_question(query: str) -> str | None:
    """Return deterministic response if query is about chatbot identity."""
    q = query.lower()
    if any(mq in q for mq in settings.META_QUESTIONS):
        return f"My name is {settings.PROJECT_NAME}, your {settings.BOT_ROLE} 🤖"
    return None
