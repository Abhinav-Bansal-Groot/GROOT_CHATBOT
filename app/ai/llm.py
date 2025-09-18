from langchain_openai import ChatOpenAI
from app.core import config


class LLM:
    def __init__(self, temperature: float = 0.0):
        self.llm = ChatOpenAI(model=config.settings.GPT_MODEL, temperature=temperature, api_key=config.settings.OPENAI_API_KEY)


    def generate(self, prompt: str) -> str:
        # ChatOpenAI.invoke returns a result object with content attribute
        result = self.llm.invoke(prompt)
        # Some langchain wrappers place the text at `.content` or `.message.content`, handle common cases
        if hasattr(result, "content"):
            return result.content
        if hasattr(result, "message") and hasattr(result.message, "content"):
            return result.message.content
        # Fallback to str(result)
        return str(result)